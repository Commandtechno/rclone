package dfs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rclone/rclone/backend/dfs/api"
)

const fakeToken = "Bot fake-token"

// fakeDiscord implements just enough of the Discord API and CDN for
// the backend: threads, upload slots, messages and ranged downloads.
type fakeDiscord struct {
	srv *httptest.Server

	mu          sync.Mutex
	nextID      uint64
	uploads     map[string][]byte        // upload filename -> bytes
	attachments map[string][]byte        // attachment id -> bytes
	messages    map[string][]api.Message // channel id -> messages
	threads     int
	archived    int
	slotCalls   int
	rateLimit   int  // answer this many slot requests with a 429 first
	noSlots     bool // reject upload slot requests (forces multipart)
}

func newFakeDiscord(t testing.TB) *fakeDiscord {
	d := &fakeDiscord{
		// newer than legacyChunkCutoff, and unique per server so stale
		// chunk_urls rows from earlier runs don't collide
		nextID:      1500000000000000000 + uint64(time.Now().UnixNano())%1e17,
		uploads:     map[string][]byte{},
		attachments: map[string][]byte{},
		messages:    map[string][]api.Message{},
	}

	api := http.NewServeMux()
	api.HandleFunc("POST /channels/{id}/threads", d.createThread)
	api.HandleFunc("PATCH /channels/{id}", d.editChannel)
	api.HandleFunc("POST /channels/{id}/attachments", d.uploadSlots)
	api.HandleFunc("POST /channels/{id}/messages", d.createMessage)
	api.HandleFunc("GET /channels/{id}/messages", d.listMessages)

	mux := http.NewServeMux()
	mux.Handle("/api/", http.StripPrefix("/api", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != fakeToken {
			writeJSON(w, http.StatusUnauthorized, api401)
			return
		}
		api.ServeHTTP(w, r)
	})))
	mux.HandleFunc("PUT /upload/{name}", d.put)
	mux.HandleFunc("GET /cdn/{id}", d.get)

	d.srv = httptest.NewServer(mux)
	t.Cleanup(d.srv.Close)
	return d
}

var api401 = map[string]any{"message": "401: Unauthorized", "code": 0}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (d *fakeDiscord) apiURL() string {
	return d.srv.URL + "/api"
}

// snowflake must be called with d.mu held
func (d *fakeDiscord) snowflake() string {
	d.nextID++
	return strconv.FormatUint(d.nextID, 10)
}

func (d *fakeDiscord) createThread(w http.ResponseWriter, r *http.Request) {
	var req api.ThreadCreate
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Type != api.ChannelTypePrivateThread {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "bad thread"})
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.threads++
	writeJSON(w, http.StatusCreated, api.Channel{ID: d.snowflake()})
}

func (d *fakeDiscord) editChannel(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.archived++
	writeJSON(w, http.StatusOK, api.Channel{ID: r.PathValue("id")})
}

func (d *fakeDiscord) uploadSlots(w http.ResponseWriter, r *http.Request) {
	var req api.UploadSlotsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.slotCalls++
	if d.noSlots {
		writeJSON(w, http.StatusForbidden, map[string]any{"message": "Missing Access", "code": 50001})
		return
	}
	if d.rateLimit > 0 {
		d.rateLimit--
		writeJSON(w, http.StatusTooManyRequests, map[string]any{"message": "You are being rate limited.", "retry_after": 0.05})
		return
	}
	if len(req.Files) > attachmentsPerMessage {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "too many files"})
		return
	}
	var resp api.UploadSlotsResponse
	for range req.Files {
		name := d.snowflake()
		resp.Attachments = append(resp.Attachments, api.UploadSlot{
			UploadURL:      d.srv.URL + "/upload/" + name,
			UploadFilename: name,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

func (d *fakeDiscord) put(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "" {
		http.Error(w, "token leaked to cdn", http.StatusBadRequest)
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.uploads[r.PathValue("name")] = data
	w.WriteHeader(http.StatusOK)
}

// attach must be called with d.mu held
func (d *fakeDiscord) attach(data []byte) api.Attachment {
	id := d.snowflake()
	d.attachments[id] = data
	ex := strconv.FormatInt(time.Now().Add(24*time.Hour).Unix(), 16)
	return api.Attachment{
		ID:       id,
		Filename: "file",
		Size:     int64(len(data)),
		URL:      fmt.Sprintf("%s/cdn/%s?ex=%s", d.srv.URL, id, ex),
	}
}

func (d *fakeDiscord) createMessage(w http.ResponseWriter, r *http.Request) {
	channelID := r.PathValue("id")
	var attachments [][]byte

	if r.Header.Get("Content-Type") == "application/json" {
		var req api.MessageCreate
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
			return
		}
		d.mu.Lock()
		for _, ref := range req.Attachments {
			data, ok := d.uploads[ref.UploadedFilename]
			if !ok {
				d.mu.Unlock()
				writeJSON(w, http.StatusBadRequest, map[string]any{"message": "unknown upload " + ref.UploadedFilename})
				return
			}
			delete(d.uploads, ref.UploadedFilename)
			attachments = append(attachments, data)
		}
		d.mu.Unlock()
	} else {
		file, _, err := r.FormFile("files[0]")
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
			return
		}
		data, _ := io.ReadAll(file)
		attachments = append(attachments, data)
	}

	if len(attachments) == 0 || len(attachments) > attachmentsPerMessage {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "bad attachment count"})
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	m := api.Message{ID: d.snowflake(), ChannelID: channelID}
	for _, data := range attachments {
		m.Attachments = append(m.Attachments, d.attach(data))
	}
	d.messages[channelID] = append(d.messages[channelID], m)
	writeJSON(w, http.StatusOK, m)
}

func (d *fakeDiscord) listMessages(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	msgs := d.messages[r.PathValue("id")]
	around := r.URL.Query().Get("around")
	// return up to 100 messages centred on around
	pos := 0
	for i, m := range msgs {
		if m.ID <= around {
			pos = i
		}
	}
	lo := max(pos-50, 0)
	hi := min(lo+100, len(msgs))
	out := make([]api.Message, 0, hi-lo)
	for _, m := range msgs[lo:hi] {
		// fresh urls, as Discord signs them on every fetch
		fresh := api.Message{ID: m.ID, ChannelID: m.ChannelID}
		for _, a := range m.Attachments {
			a.URL = fmt.Sprintf("%s/cdn/%s?ex=%s", d.srv.URL, a.ID, strconv.FormatInt(time.Now().Add(24*time.Hour).Unix(), 16))
			fresh.Attachments = append(fresh.Attachments, a)
		}
		out = append(out, fresh)
	}
	writeJSON(w, http.StatusOK, out)
}

func (d *fakeDiscord) get(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	data, ok := d.attachments[r.PathValue("id")]
	d.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
}

// apiMessage wraps attachments made with attach in a message
func apiMessage(channelID string, attachments ...api.Attachment) api.Message {
	return api.Message{ID: attachments[0].ID, ChannelID: channelID, Attachments: attachments}
}

// stored returns the bytes Discord holds for a "channel/attachment" chunk
func (d *fakeDiscord) stored(chunk string) []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, msgs := range d.messages {
		for _, m := range msgs {
			for _, a := range m.Attachments {
				if m.ChannelID+"/"+a.ID == chunk {
					return d.attachments[a.ID]
				}
			}
		}
	}
	return nil
}

// testDatabaseURL returns a scratch postgres to test against, creating
// the dfs tables (as gorm would) if needed
func testDatabaseURL(t testing.TB) string {
	dbURL := os.Getenv("DFS_TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("set DFS_TEST_DATABASE_URL to a scratch postgres database to run dfs tests")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, `
		create table if not exists directories (
			parent_id uuid, created_at timestamptz, deleted_at timestamptz,
			id uuid primary key, name text, encrypted boolean);
		create table if not exists files (
			parent_id uuid, created_at timestamptz, modified_at timestamptz, deleted_at timestamptz,
			id uuid primary key, name text, type text, size bigint, chunks text, hashes text,
			encryption_key bytea);
		create table if not exists chunk_urls (
			attachment_id text primary key, channel_id text, url text, expires_at timestamptz);`)
	if err != nil {
		t.Fatal(err)
	}
	return dbURL
}

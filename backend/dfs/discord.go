package dfs

// Talking to Discord: threads to hold chunks, uploading chunk bytes
// straight to the CDN bucket, and turning stored chunk references back
// into (expiring) CDN urls.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/backend/dfs/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

var retryErrorCodes = []int{
	429, // Too Many Requests
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried. It returns the err as a convenience
func shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		var retryAfter time.Duration
		var apiErr *api.Error
		if errors.As(err, &apiErr) && apiErr.RetryAfter > 0 {
			retryAfter = time.Duration(apiErr.RetryAfter * float64(time.Second))
		} else if secs, perr := strconv.ParseFloat(resp.Header.Get("Retry-After"), 64); perr == nil {
			retryAfter = time.Duration(secs * float64(time.Second))
		}
		if retryAfter > 0 {
			return true, pacer.RetryAfterError(err, retryAfter)
		}
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

func errorHandler(resp *http.Response) error {
	errResp := &api.Error{Status: resp.StatusCode}
	body, err := rest.ReadBody(resp)
	if err == nil && len(body) > 0 {
		_ = json.Unmarshal(body, errResp)
	}
	if errResp.Message == "" {
		errResp.Message = strings.TrimSpace(resp.Status + " " + string(body))
	}
	return errResp
}

// acquireThread returns the thread to put size more bytes of chunks in,
// starting a new one when the current thread is full
func (f *Fs) acquireThread(ctx context.Context, size int64) (string, error) {
	t := f.thread
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.id != "" && t.used+size <= int64(f.opt.ThreadSize) {
		t.used += size
		return t.id, nil
	}

	if old := t.id; old != "" {
		go f.archiveThread(old)
	}

	opts := rest.Opts{
		Method: "POST",
		Path:   "/channels/" + f.opt.ChannelID + "/threads",
	}
	req := api.ThreadCreate{
		Name:                "rclone " + time.Now().UTC().Format("2006-01-02 15:04:05"),
		Type:                api.ChannelTypePrivateThread,
		AutoArchiveDuration: 60,
	}
	var thread api.Channel
	err := f.pacer.Call(func() (bool, error) {
		resp, err := f.srv.CallJSON(ctx, &opts, &req, &thread)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return "", fmt.Errorf("create thread: %w", err)
	}

	t.id, t.used = thread.ID, size
	return thread.ID, nil
}

// archiveThread hides a full thread from the channel's active list
func (f *Fs) archiveThread(threadID string) {
	ctx := context.Background()
	opts := rest.Opts{
		Method:     "PATCH",
		Path:       "/channels/" + threadID,
		NoResponse: true,
	}
	req := api.ChannelEdit{Archived: true}
	err := f.pacer.Call(func() (bool, error) {
		resp, err := f.srv.CallJSON(ctx, &opts, &req, nil)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		fs.Debugf(f, "failed to archive thread %s: %v", threadID, err)
	}
}

// uploadSlots asks Discord for signed urls so chunk bytes go straight
// to the CDN bucket instead of through the API proxy
func (f *Fs) uploadSlots(ctx context.Context, threadID string, sizes []int64) ([]api.UploadSlot, error) {
	req := api.UploadSlotsRequest{Files: make([]api.UploadSlotFile, len(sizes))}
	for i, size := range sizes {
		req.Files[i] = api.UploadSlotFile{ID: strconv.Itoa(i), Filename: "file", FileSize: size}
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   "/channels/" + threadID + "/attachments",
	}
	var resp api.UploadSlotsResponse
	err := f.pacer.Call(func() (bool, error) {
		httpResp, err := f.srv.CallJSON(ctx, &opts, &req, &resp)
		return shouldRetry(ctx, httpResp, err)
	})
	if err != nil {
		return nil, err
	}
	// response preserves request order
	if len(resp.Attachments) != len(sizes) {
		return nil, fmt.Errorf("expected %d upload slots, got %d", len(sizes), len(resp.Attachments))
	}
	return resp.Attachments, nil
}

// putChunk uploads data to an upload slot's url
func (f *Fs) putChunk(ctx context.Context, uploadURL string, data []byte) error {
	size := int64(len(data))
	opts := rest.Opts{
		Method:        "PUT",
		RootURL:       uploadURL,
		ContentType:   "application/octet-stream",
		ContentLength: &size,
		NoResponse:    true,
	}
	return f.dataPacer.Call(func() (bool, error) {
		opts.Body = bytes.NewReader(data)
		resp, err := f.cdn.Call(ctx, &opts)
		return shouldRetry(ctx, resp, err)
	})
}

// postAttachments sends a message made of attachments already PUT to
// their upload slots
func (f *Fs) postAttachments(ctx context.Context, threadID string, uploadedFilenames []string) (*api.Message, error) {
	req := api.MessageCreate{Attachments: make([]api.AttachmentRef, len(uploadedFilenames))}
	for i, name := range uploadedFilenames {
		req.Attachments[i] = api.AttachmentRef{ID: strconv.Itoa(i), Filename: "file", UploadedFilename: name}
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   "/channels/" + threadID + "/messages",
	}
	var m api.Message
	err := f.pacer.Call(func() (bool, error) {
		resp, err := f.srv.CallJSON(ctx, &opts, &req, &m)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, err
	}
	if len(m.Attachments) != len(uploadedFilenames) {
		return nil, fmt.Errorf("expected %d attachments in message, got %d", len(uploadedFilenames), len(m.Attachments))
	}
	f.cacheAttachments(ctx, threadID, m.Attachments)
	return &m, nil
}

// postMultipart sends data as a single attachment the old fashioned
// way, through the API. Used if upload slots stop working.
func (f *Fs) postMultipart(ctx context.Context, threadID string, data []byte) (*api.Message, error) {
	opts := rest.Opts{
		Method:               "POST",
		Path:                 "/channels/" + threadID + "/messages",
		MultipartContentName: "files[0]",
		MultipartFileName:    "file",
	}
	var m api.Message
	err := f.pacer.Call(func() (bool, error) {
		opts.Body = bytes.NewReader(data)
		resp, err := f.srv.CallJSON(ctx, &opts, nil, &m)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, err
	}
	if len(m.Attachments) != 1 {
		return nil, fmt.Errorf("expected 1 attachment in message, got %d", len(m.Attachments))
	}
	f.cacheAttachments(ctx, threadID, m.Attachments)
	return &m, nil
}

type cachedURL struct {
	url     string
	expires time.Time
}

func (u cachedURL) usable() bool {
	return u.url != "" && u.expires.After(time.Now().Add(2*time.Minute))
}

// urlExpiry reads the ex query parameter (hex unix seconds) of a CDN url
func urlExpiry(cdnURL string) time.Time {
	if u, err := url.Parse(cdnURL); err == nil {
		if ts, err := strconv.ParseInt(u.Query().Get("ex"), 16, 64); err == nil {
			return time.Unix(ts, 0)
		}
	}
	// unsigned url - don't trust it for long
	return time.Now().Add(10 * time.Minute)
}

func (f *Fs) cacheAttachments(ctx context.Context, channelID string, attachments []api.Attachment) {
	rows := make([]chunkURL, 0, len(attachments))
	f.urls.mu.Lock()
	for _, a := range attachments {
		if a.URL == "" {
			continue
		}
		expires := urlExpiry(a.URL)
		f.urls.urls[a.ID] = cachedURL{url: a.URL, expires: expires}
		rows = append(rows, chunkURL{AttachmentID: a.ID, ChannelID: channelID, URL: a.URL, ExpiresAt: expires})
	}
	f.urls.mu.Unlock()
	f.saveChunkURLs(ctx, rows)
}

func (f *Fs) forgetURL(attachmentID string) {
	f.urls.mu.Lock()
	delete(f.urls.urls, attachmentID)
	f.urls.mu.Unlock()
}

// chunkURL resolves a "channelID/attachmentID" chunk reference to a
// CDN url, via the memory cache, the chunk_urls table, then Discord.
//
// Messages are fetched 100 at a time around the attachment (attachment
// and message snowflakes are close) which warms the cache for the
// neighbouring chunks too.
func (f *Fs) chunkURL(ctx context.Context, chunk string, refresh bool) (string, error) {
	channelID, attachmentID, ok := strings.Cut(chunk, "/")
	if !ok || channelID == "" || attachmentID == "" {
		return "", fmt.Errorf("bad chunk reference %q", chunk)
	}

	if !refresh {
		f.urls.mu.Lock()
		u := f.urls.urls[attachmentID]
		f.urls.mu.Unlock()
		if u.usable() {
			return u.url, nil
		}

		if dbURL, expires, ok := f.loadChunkURL(ctx, attachmentID); ok {
			u := cachedURL{url: dbURL, expires: expires}
			if u.usable() {
				f.urls.mu.Lock()
				f.urls.urls[attachmentID] = u
				f.urls.mu.Unlock()
				return u.url, nil
			}
		}
	}

	opts := rest.Opts{
		Method: "GET",
		Path:   "/channels/" + channelID + "/messages",
		Parameters: url.Values{
			"limit":  {"100"},
			"around": {attachmentID},
		},
	}
	var messages []api.Message
	err := f.pacer.Call(func() (bool, error) {
		resp, err := f.srv.CallJSON(ctx, &opts, nil, &messages)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return "", fmt.Errorf("resolve chunk %s: %w", chunk, err)
	}

	var attachments []api.Attachment
	for _, m := range messages {
		attachments = append(attachments, m.Attachments...)
	}
	f.cacheAttachments(ctx, channelID, attachments)

	for _, a := range attachments {
		if a.ID == attachmentID && a.URL != "" {
			return a.URL, nil
		}
	}
	return "", fmt.Errorf("resolve chunk %s: attachment not found", chunk)
}

// fetchChunk downloads bytes [lo, hi] of a stored chunk
func (f *Fs) fetchChunk(ctx context.Context, chunk string, lo, hi int64) ([]byte, error) {
	want := hi - lo + 1
	refreshed := false
	for {
		cdnURL, err := f.chunkURL(ctx, chunk, refreshed)
		if err != nil {
			return nil, err
		}

		opts := rest.Opts{
			Method:  "GET",
			RootURL: cdnURL,
			Options: []fs.OpenOption{&fs.RangeOption{Start: lo, End: hi}},
		}
		var data []byte
		var status int
		err = f.dataPacer.Call(func() (bool, error) {
			resp, err := f.cdn.Call(ctx, &opts)
			if resp != nil {
				status = resp.StatusCode
			}
			if err != nil {
				if status == http.StatusForbidden || status == http.StatusNotFound {
					// probably an expired url
					return false, err
				}
				return shouldRetry(ctx, resp, err)
			}
			data, err = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				return shouldRetry(ctx, resp, err)
			}
			if resp.StatusCode == http.StatusOK && int64(len(data)) > hi {
				// range ignored, so we got the whole chunk
				data = data[lo : hi+1]
			}
			if int64(len(data)) != want {
				return true, fmt.Errorf("chunk %s: expected %d bytes, got %d", chunk, want, len(data))
			}
			return false, nil
		})

		// cached urls go stale (expired, or point at a dead host), so
		// resolve a fresh one before giving up
		if err != nil && !refreshed && ctx.Err() == nil {
			_, attachmentID, _ := strings.Cut(chunk, "/")
			f.forgetURL(attachmentID)
			refreshed = true
			continue
		}
		return data, err
	}
}

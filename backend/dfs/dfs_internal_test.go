package dfs

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"encoding/hex"
	"io"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/lib/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestFs makes an Fs rooted in a fresh directory, with 1 KiB chunks
// and 25 KiB threads so small files exercise batching and rollover
func newTestFs(t *testing.T) (*Fs, *fakeDiscord) {
	dbURL := testDatabaseURL(t)
	d := newFakeDiscord(t)

	oldChunkSize := chunkSize
	chunkSize = 1024
	t.Cleanup(func() { chunkSize = oldChunkSize })

	ctx := context.Background()
	root := "dfs-internal-" + random.String(8)
	f, err := NewFs(ctx, "TestDFS", root, configmap.Simple{
		"discord_token": fakeToken,
		"channel_id":    "1",
		"database_url":  dbURL,
		"api_url":       d.apiURL(),
		"thread_size":   "25Ki",
	})
	require.NoError(t, err)
	require.NoError(t, f.Mkdir(ctx, ""))
	t.Cleanup(func() {
		assert.NoError(t, f.Features().Purge(ctx, ""))
		assert.NoError(t, f.(fs.Shutdowner).Shutdown(ctx))
	})
	return f.(*Fs), d
}

func randomBytes(n int) []byte {
	data := make([]byte, n)
	_, _ = rand.New(rand.NewSource(int64(n))).Read(data)
	return data
}

func put(t *testing.T, f *Fs, remote string, data []byte, size int64) *Object {
	ctx := context.Background()
	src := object.NewStaticObjectInfo(remote, time.Now(), size, true, nil, nil)
	var (
		o   fs.Object
		err error
	)
	if size < 0 {
		o, err = f.PutStream(ctx, bytes.NewReader(data), src)
	} else {
		o, err = f.Put(ctx, bytes.NewReader(data), src)
	}
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), o.Size())
	return o.(*Object)
}

func readRange(t *testing.T, o fs.Object, options ...fs.OpenOption) []byte {
	rc, err := o.Open(context.Background(), options...)
	require.NoError(t, err)
	defer func() { require.NoError(t, rc.Close()) }()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	return got
}

// checkReads reads o back whole and in pieces that straddle chunk
// boundaries, from a cold url cache
func checkReads(t *testing.T, o fs.Object, want []byte) {
	n := int64(len(want))
	assert.Equal(t, want, readRange(t, o))
	for _, r := range []fs.RangeOption{
		{Start: 0, End: 0},
		{Start: 1023, End: 1024},
		{Start: 1000, End: 1100},
		{Start: 5000, End: 20000},
		{Start: n - 1, End: n - 1},
		{Start: -1, End: 10}, // last 10 bytes
	} {
		r := r
		start, end := r.Start, r.End
		if start < 0 {
			start, end = n-end, n-1
		}
		if start >= n {
			continue
		}
		end = min(end, n-1)
		assert.Equal(t, want[start:end+1], readRange(t, o, &r), "range %v", r)
	}
	if n > 3000 {
		assert.Equal(t, want[3000:], readRange(t, o, &fs.SeekOption{Offset: 3000}))
	}
}

func TestStoredChunkSize(t *testing.T) {
	const MiB = 1024 * 1024
	legacy, current := "1/1000000000000000000", "1/1500000000000000000"
	for _, test := range []struct {
		size   int64
		chunks []string
		want   int64
	}{
		{size: 5, chunks: []string{current}, want: 5},
		{size: 30 * MiB, chunks: []string{legacy, legacy}, want: 25 * MiB},
		{size: 30 * MiB, chunks: []string{current, current, current}, want: 10 * MiB},
		// era guess is wrong, size and chunk count still decide
		{size: 30 * MiB, chunks: []string{current, current}, want: 25 * MiB},
		{size: 50 * MiB, chunks: []string{legacy, legacy, legacy, legacy, legacy}, want: 10 * MiB},
		{size: 100 * MiB, chunks: []string{current, current}, want: 0},
	} {
		o := &Object{size: test.size, chunks: test.chunks}
		got, err := o.storedChunkSize()
		if test.want == 0 {
			assert.Error(t, err)
			continue
		}
		require.NoError(t, err)
		assert.Equal(t, test.want, got, "size %d in %d chunks", test.size, len(test.chunks))
	}
}

func TestMultiChunkRoundTrip(t *testing.T) {
	f, d := newTestFs(t)
	want := randomBytes(25*1024 + 123) // 26 chunks, 3 messages
	o := put(t, f, "dir/big.bin", want, int64(len(want)))

	require.Len(t, o.chunks, 26)
	file, err := f.findFileByID(context.Background(), o)
	require.NoError(t, err)
	for i, chunk := range file.Chunks {
		start := int64(i) * 1024
		sum := md5.Sum(want[start:min(start+1024, int64(len(want)))])
		assert.Equal(t, hex.EncodeToString(sum[:]), file.Hashes[i])
		assert.Equal(t, sum, md5.Sum(d.stored(chunk)), "chunk %d", i)
	}
	assert.Equal(t, 2, d.threads, "25 KiB threads should roll over once")

	// all url caches cold: resolve through the messages endpoint
	f.urls.urls = map[string]cachedURL{}
	f.noURLTable.Store(true)
	checkReads(t, o, want)

	// memory cold but chunk_urls warm
	f.urls.urls = map[string]cachedURL{}
	f.noURLTable.Store(false)
	checkReads(t, o, want)

	// a fresh listing sees the same thing
	f.DirCacheFlush()
	o2, err := f.NewObject(context.Background(), "dir/big.bin")
	require.NoError(t, err)
	checkReads(t, o2, want)
}

func TestPutStreamUnknownSize(t *testing.T) {
	f, d := newTestFs(t)
	want := randomBytes(12*1024 + 512)
	o := put(t, f, "stream.bin", want, -1)
	require.Len(t, o.chunks, 13)
	assert.Equal(t, 13, d.slotCalls, "unknown size gets a slot per chunk")
	checkReads(t, o, want)
}

func TestEmptyFile(t *testing.T) {
	f, d := newTestFs(t)
	o := put(t, f, "empty", nil, 0)
	assert.Empty(t, o.chunks)
	assert.Empty(t, readRange(t, o))
	put(t, f, "empty-stream", nil, -1)
	put(t, f, "one-batch-stream", randomBytes(10*1024), -1)
	assert.Equal(t, 1, d.threads, "no thread for empty input or a trailing empty batch")
}

func TestUpdateReplacesContent(t *testing.T) {
	f, _ := newTestFs(t)
	put(t, f, "file", randomBytes(3000), 3000)
	want := randomBytes(5000)
	o := put(t, f, "file", want, 5000)
	entries, err := f.List(context.Background(), "")
	require.NoError(t, err)
	assert.Len(t, entries, 1, "update shouldn't make a duplicate")
	checkReads(t, o, want)
}

// fsNewEncrypt is how fs-new's dfs encrypts: AES-CTR with a zero IV,
// restarted for each chunk
func fsNewEncrypt(t *testing.T, key, chunk []byte) []byte {
	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	out := make([]byte, len(chunk))
	cipher.NewCTR(block, make([]byte, aes.BlockSize)).XORKeyStream(out, chunk)
	return out
}

func TestEncryptedDir(t *testing.T) {
	f, d := newTestFs(t)
	ctx := context.Background()

	// mark the parent encrypted, files in subdirectories inherit it
	require.NoError(t, f.Mkdir(ctx, "secret/nested"))
	secretID, err := f.dirCache.FindDir(ctx, "secret", false)
	require.NoError(t, err)
	_, err = f.db.Exec(ctx, `update directories set encrypted = true where id = $1`, secretID)
	require.NoError(t, err)

	want := randomBytes(4*1024 + 77)
	o := put(t, f, "secret/nested/file.bin", want, int64(len(want)))
	require.Len(t, o.key, 32)
	for i, chunk := range o.chunks {
		start := i * 1024
		plain := want[start:min(start+1024, len(want))]
		assert.Equal(t, fsNewEncrypt(t, o.key, plain), d.stored(chunk), "chunk %d", i)
	}
	checkReads(t, o, want)

	// a rewrite must not reuse the keystream
	oldKey := o.key
	o = put(t, f, "secret/nested/file.bin", want, int64(len(want)))
	assert.NotEqual(t, oldKey, o.key)
	checkReads(t, o, want)

	plain := put(t, f, "plain.bin", want[:100], 100)
	assert.Nil(t, plain.key)
}

func TestMultipartFallback(t *testing.T) {
	f, d := newTestFs(t)
	d.noSlots = true
	want := randomBytes(3*1024 + 1)
	o := put(t, f, "fallback.bin", want, int64(len(want)))
	assert.True(t, f.cloudBroken.Load())
	assert.Equal(t, 1, d.slotCalls, "only the first slot request should be tried")
	checkReads(t, o, want)
}

func TestRateLimitRetry(t *testing.T) {
	f, d := newTestFs(t)
	d.rateLimit = 2
	want := randomBytes(2048)
	o := put(t, f, "limited.bin", want, int64(len(want)))
	assert.False(t, f.cloudBroken.Load(), "429 should be retried, not fall back")
	checkReads(t, o, want)
}

func TestExpiredURLRefreshed(t *testing.T) {
	f, d := newTestFs(t)
	want := randomBytes(2500)
	o := put(t, f, "expiring.bin", want, int64(len(want)))

	// cache urls that look valid but 404
	for _, chunk := range o.chunks {
		_, attachmentID, _ := strings.Cut(chunk, "/")
		f.urls.urls[attachmentID] = cachedURL{url: d.srv.URL + "/cdn/gone", expires: time.Now().Add(time.Hour)}
	}
	assert.Equal(t, want, readRange(t, o))
}

func TestMoveCopyPurge(t *testing.T) {
	f, _ := newTestFs(t)
	ctx := context.Background()
	want := randomBytes(3000)
	o := put(t, f, "a/file", want, int64(len(want)))

	copied, err := f.Copy(ctx, o, "b/copy")
	require.NoError(t, err)
	assert.Equal(t, o.chunks, copied.(*Object).chunks, "copies share chunks")
	assert.Equal(t, want, readRange(t, copied))

	// move over the copy replaces it
	moved, err := f.Move(ctx, o, "b/copy")
	require.NoError(t, err)
	entries, err := f.List(ctx, "b")
	require.NoError(t, err)
	assert.Len(t, entries, 1)
	assert.Equal(t, want, readRange(t, moved))

	require.NoError(t, f.DirMove(ctx, f, "b", "c/d"))
	_, err = f.NewObject(ctx, "c/d/copy")
	require.NoError(t, err)

	require.NoError(t, f.Purge(ctx, "c"))
	_, err = f.NewObject(ctx, "c/d/copy")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)

	// purged rows are soft deleted, like gorm does
	var deleted int
	require.NoError(t, f.db.QueryRow(ctx, `select count(*) from files where id = $1 and deleted_at is not null`, moved.(*Object).id).Scan(&deleted))
	assert.Equal(t, 1, deleted)
}

// rows written by fsexp (gorm) with nothing but the columns it sets
func TestReadsFsexpRows(t *testing.T) {
	f, d := newTestFs(t)
	ctx := context.Background()
	dirID, err := f.dirCache.RootID(ctx, false)
	require.NoError(t, err)

	want := randomBytes(1500)
	d.mu.Lock()
	m1 := d.attach(want[:1024])
	m2 := d.attach(want[1024:])
	d.messages["77"] = append(d.messages["77"], apiMessage("77", m1, m2))
	d.mu.Unlock()

	_, err = f.db.Exec(ctx, `insert into files (parent_id, created_at, modified_at, id, name, type, size, chunks, hashes, encryption_key)
		values ($1, now(), now(), $2, 'from-fsexp.bin', 'application/octet-stream', 1500, $3, ',', null)`,
		dirID, uuid.NewString(), "77/"+m1.ID+",77/"+m2.ID)
	require.NoError(t, err)

	o, err := f.NewObject(ctx, "from-fsexp.bin")
	require.NoError(t, err)
	assert.Equal(t, want, readRange(t, o))
}

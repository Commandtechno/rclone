package dfs

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Chunks uploaded before this attachment snowflake were 25 MiB
const legacyChunkCutoff = 1329698388786151456

const legacyChunkSize = 25 * 1024 * 1024

// storedChunkSize works out the chunk size o was uploaded with
func (o *Object) storedChunkSize() (int64, error) {
	n := int64(len(o.chunks))
	if n <= 1 {
		return max(o.size, 1), nil
	}

	candidates := []int64{chunkSize, legacyChunkSize}
	_, attachmentID, _ := strings.Cut(o.chunks[0], "/")
	if id, err := strconv.ParseUint(attachmentID, 10, 64); err == nil && id < legacyChunkCutoff {
		candidates[0], candidates[1] = candidates[1], candidates[0]
	}
	for _, cs := range candidates {
		if (n-1)*cs < o.size && o.size <= n*cs {
			return cs, nil
		}
	}
	return 0, fmt.Errorf("can't work out chunk size for %d bytes in %d chunks", o.size, n)
}

// newCTR returns the keystream for a chunk encrypted by fs-new's dfs:
// AES-CTR with a zero IV, restarted for every chunk, positioned at
// offset into the chunk
func newCTR(key []byte, offset int64) (cipher.Stream, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	iv := make([]byte, aes.BlockSize)
	binary.BigEndian.PutUint64(iv[8:], uint64(offset/aes.BlockSize))
	stream := cipher.NewCTR(block, iv)
	if skip := offset % aes.BlockSize; skip > 0 {
		discard := make([]byte, skip)
		stream.XORKeyStream(discard, discard)
	}
	return stream, nil
}

type chunkResult struct {
	data []byte
	err  error
}

// chunkReader streams a byte range of an object, fetching up to
// download_concurrency chunks ahead of the reader
type chunkReader struct {
	cancel  context.CancelFunc
	futures chan chan chunkResult
	tokens  chan struct{} // one per chunk fetched or buffered
	prodErr error         // set before futures is closed

	cur  []byte
	held bool // cur holds a token
	err  error
}

func (o *Object) newChunkReader(ctx context.Context, offset, limit int64) (io.ReadCloser, error) {
	cs, err := o.storedChunkSize()
	if err != nil {
		return nil, err
	}

	end := o.size // exclusive
	if limit >= 0 {
		end = min(offset+limit, o.size)
	}

	ctx, cancel := context.WithCancel(ctx)
	r := &chunkReader{
		cancel:  cancel,
		futures: make(chan chan chunkResult, o.fs.opt.DownloadConcurrency),
		tokens:  make(chan struct{}, o.fs.opt.DownloadConcurrency),
	}
	if offset >= end {
		close(r.futures)
		return r, nil
	}

	go r.produce(ctx, o, cs, offset, end)
	return r, nil
}

func (r *chunkReader) produce(ctx context.Context, o *Object, cs, offset, end int64) {
	defer close(r.futures)

	for i := offset / cs; i*cs < end; i++ {
		if i >= int64(len(o.chunks)) || o.chunks[i] == "" {
			r.prodErr = fmt.Errorf("chunk %d missing (incomplete upload)", i)
			return
		}
		chunk := o.chunks[i]

		select {
		case r.tokens <- struct{}{}:
		case <-ctx.Done():
			r.prodErr = ctx.Err()
			return
		}

		// resolve in order here so a cold cache costs one API call per
		// ~100 messages rather than one per concurrent fetch
		if _, err := o.fs.chunkURL(ctx, chunk, false); err != nil {
			r.prodErr = err
			return
		}

		lo := max(offset-i*cs, 0)
		hi := min(end-i*cs, cs) - 1
		fut := make(chan chunkResult, 1)
		go func() {
			data, err := o.fs.fetchChunk(ctx, chunk, lo, hi)
			if err == nil && o.key != nil {
				var stream cipher.Stream
				stream, err = newCTR(o.key, lo)
				if err == nil {
					stream.XORKeyStream(data, data)
				}
			}
			fut <- chunkResult{data: data, err: err}
		}()

		select {
		case r.futures <- fut:
		case <-ctx.Done():
			r.prodErr = ctx.Err()
			return
		}
	}
}

// Read implements io.Reader
func (r *chunkReader) Read(p []byte) (int, error) {
	for len(r.cur) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		if r.held {
			<-r.tokens
			r.held = false
		}

		fut, ok := <-r.futures
		if !ok {
			r.err = r.prodErr
			if r.err == nil {
				r.err = io.EOF
			}
			continue
		}

		res := <-fut
		r.held = true
		if res.err != nil {
			r.err = res.err
			continue
		}
		r.cur = res.data
	}

	n := copy(p, r.cur)
	r.cur = r.cur[n:]
	return n, nil
}

// Close implements io.Closer
func (r *chunkReader) Close() error {
	r.cancel()
	return nil
}

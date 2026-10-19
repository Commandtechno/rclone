package dfs

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/rclone/rclone/backend/dfs/api"
	"github.com/rclone/rclone/fs"
	"golang.org/x/sync/errgroup"
)

const attachmentsPerMessage = 10

// chunkSize is what fsexp uploads with. Readers work out the chunk size
// of a file from its size and chunk count, which is only unambiguous
// with the sizes the tools have always used. A var so tests can shrink it.
var chunkSize int64 = 10 * 1024 * 1024

// uploadResult collects chunk references as uploads finish
type uploadResult struct {
	mu     sync.Mutex
	chunks []string
	hashes []string
}

func (r *uploadResult) set(i int, ref, hash string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for len(r.chunks) <= i {
		r.chunks = append(r.chunks, "")
		r.hashes = append(r.hashes, "")
	}
	r.chunks[i] = ref
	r.hashes[i] = hash
}

// cloudChunk is a chunk PUT to an upload slot, waiting to be posted
type cloudChunk struct {
	idx      int
	filename string
	hash     string
}

// uploadChunks stores in (size bytes, or -1 if unknown) as chunks,
// encrypting them with key if set. Every 10 chunks are posted as one
// message, so a batch shares a thread and an upload slots request.
func (f *Fs) uploadChunks(ctx context.Context, in io.Reader, size int64, key []byte) (chunks, hashes []string, written int64, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, gCtx := errgroup.WithContext(ctx)
	// bounds the chunk buffers in memory
	tokens := make(chan struct{}, f.opt.UploadConcurrency)
	result := &uploadResult{}
	br := bufio.NewReader(in)

	readErr := func() error {
		for idx, eof := 0, false; !eof; {
			// don't start a thread for nothing (empty input, or input
			// ending exactly on a batch boundary)
			if _, err := br.Peek(1); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return err
			}

			// work out the batch's chunk sizes up front if we can, so one
			// request gets upload slots for all of them
			var sizes []int64
			batchBytes := attachmentsPerMessage * chunkSize
			if size >= 0 {
				batchBytes = 0
				for j := range attachmentsPerMessage {
					start := int64(idx+j) * chunkSize
					if start >= size {
						break
					}
					sizes = append(sizes, min(size-start, chunkSize))
					batchBytes += sizes[j]
				}
			}

			threadID, err := f.acquireThread(gCtx, batchBytes)
			if err != nil {
				return err
			}

			var slots []api.UploadSlot
			if len(sizes) > 0 && !f.cloudBroken.Load() {
				slots, err = f.uploadSlots(gCtx, threadID, sizes)
				if err != nil {
					if gCtx.Err() != nil {
						return err
					}
					fs.Logf(f, "upload slots unavailable, falling back to multipart: %v", err)
					f.cloudBroken.Store(true)
				}
			}

			var (
				batchMu sync.Mutex
				batch   []cloudChunk
				batchWG sync.WaitGroup
			)
			for j := 0; j < attachmentsPerMessage && !eof; j++ {
				select {
				case tokens <- struct{}{}:
				case <-gCtx.Done():
					return gCtx.Err()
				}

				buf := make([]byte, chunkSize)
				n, err := io.ReadFull(br, buf)
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					eof = true
				} else if err != nil {
					<-tokens
					return err
				}
				if n == 0 {
					<-tokens
					break
				}

				data := buf[:n]
				written += int64(n)
				i := idx
				idx++

				var slot *api.UploadSlot
				if j < len(slots) && sizes[j] == int64(n) {
					slot = &slots[j]
				}

				batchWG.Add(1)
				g.Go(func() error {
					defer batchWG.Done()
					defer func() { <-tokens }()

					if key != nil {
						stream, err := newCTR(key, 0)
						if err != nil {
							return err
						}
						stream.XORKeyStream(data, data)
					}
					sum := md5.Sum(data)
					hash := hex.EncodeToString(sum[:])

					if f.cloudBroken.Load() {
						m, err := f.postMultipart(gCtx, threadID, data)
						if err != nil {
							return fmt.Errorf("chunk %d: %w", i, err)
						}
						result.set(i, threadID+"/"+m.Attachments[0].ID, hash)
						return nil
					}

					if slot == nil {
						// size unknown up front (or the input lied about it)
						slots, err := f.uploadSlots(gCtx, threadID, []int64{int64(len(data))})
						if err != nil {
							return fmt.Errorf("chunk %d: %w", i, err)
						}
						slot = &slots[0]
					}
					if err := f.putChunk(gCtx, slot.UploadURL, data); err != nil {
						return fmt.Errorf("chunk %d: %w", i, err)
					}

					batchMu.Lock()
					batch = append(batch, cloudChunk{idx: i, filename: slot.UploadFilename, hash: hash})
					batchMu.Unlock()
					return nil
				})
			}

			g.Go(func() error {
				batchWG.Wait()
				if gCtx.Err() != nil || len(batch) == 0 {
					return nil
				}
				filenames := make([]string, len(batch))
				for k, c := range batch {
					filenames[k] = c.filename
				}
				m, err := f.postAttachments(gCtx, threadID, filenames)
				if err != nil {
					return err
				}
				for k, c := range batch {
					result.set(c.idx, threadID+"/"+m.Attachments[k].ID, c.hash)
				}
				return nil
			})
		}
		return nil
	}()

	if readErr != nil {
		cancel()
	}
	err = g.Wait()
	if readErr != nil {
		err = readErr
	}
	if err != nil {
		return nil, nil, written, err
	}
	return result.chunks, result.hashes, written, nil
}

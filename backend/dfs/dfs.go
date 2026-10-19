// Package dfs provides an interface to dfs, the chunked file store
// that keeps metadata in postgres and file contents as Discord
// attachments (the format used by fsexp and fs-new).
package dfs

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

const (
	defaultAPIURL = "https://discord.com/api/v9"
	minSleep      = 10 * time.Millisecond
	maxSleep      = 2 * time.Second
	decayConstant = 2
	userAgent     = "DiscordBot (https://rclone.org, 1)"
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "dfs",
		Description: "dfs (Discord chunk storage with postgres metadata)",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:      "discord_token",
			Help:      "Discord token.\n\nSent as the Authorization header exactly as given, so include the \"Bot \" prefix for a bot token.",
			Required:  true,
			Sensitive: true,
		}, {
			Name:     "channel_id",
			Help:     "ID of the channel chunks are uploaded to.\n\nEach upload goes in a private thread of this channel.",
			Required: true,
		}, {
			Name:      "database_url",
			Help:      "Postgres connection URL for the metadata database.\n\nE.g. postgres://user:pass@host:5432/db",
			Required:  true,
			Sensitive: true,
		}, {
			Name:     "upload_concurrency",
			Help:     "Number of chunks to upload at once per file.\n\nEach one buffers a 10 MiB chunk in memory.",
			Default:  4,
			Advanced: true,
		}, {
			Name:     "download_concurrency",
			Help:     "Number of chunks to fetch ahead per open file.\n\nEach one buffers up to a chunk (10 or 25 MiB) in memory.",
			Default:  4,
			Advanced: true,
		}, {
			Name:     "thread_size",
			Help:     "Start a new thread after this much data has been uploaded to one.",
			Default:  fs.SizeSuffix(2 * fs.Gibi),
			Advanced: true,
		}, {
			Name:     "api_url",
			Help:     "Discord API root URL.",
			Default:  defaultAPIURL,
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			// Names are stored verbatim in postgres, which only can't
			// hold NUL. Encode . and .. too since fsexp ignores them.
			Default: encoder.Base | encoder.EncodeInvalidUtf8,
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	DiscordToken        string               `config:"discord_token"`
	ChannelID           string               `config:"channel_id"`
	DatabaseURL         string               `config:"database_url"`
	UploadConcurrency   int                  `config:"upload_concurrency"`
	DownloadConcurrency int                  `config:"download_concurrency"`
	ThreadSize          fs.SizeSuffix        `config:"thread_size"`
	APIURL              string               `config:"api_url"`
	Enc                 encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a remote dfs
type Fs struct {
	name      string
	root      string
	opt       Options
	features  *fs.Features
	db        *pgxpool.Pool
	srv       *rest.Client // Discord API
	cdn       *rest.Client // upload slots and attachment downloads, unauthenticated
	pacer     *fs.Pacer
	dataPacer *fs.Pacer
	dirCache  *dircache.DirCache

	// shared with copies of the Fs made in NewFs
	thread      *threadState
	urls        *urlCache
	cloudBroken *atomic.Bool // upload slots failed, use multipart
	noURLTable  *atomic.Bool // chunk_urls table doesn't exist
}

// threadState is the thread new chunks are going to
type threadState struct {
	mu   sync.Mutex
	id   string
	used int64
}

// urlCache holds resolved CDN urls by attachment ID
type urlCache struct {
	mu   sync.Mutex
	urls map[string]cachedURL
}

// Object describes a dfs file
type Object struct {
	fs       *Fs
	remote   string
	id       string
	size     int64
	modTime  time.Time
	mimeType string
	chunks   []string
	key      []byte
}

// ------------------------------------------------------------

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("dfs root '%s'", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Precision of modification times (postgres timestamps)
func (f *Fs) Precision() time.Duration {
	return time.Microsecond
}

// Hashes returns the supported hash sets. Only per chunk md5s are
// stored, which don't make a whole file hash.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}
	if opt.UploadConcurrency < 1 {
		opt.UploadConcurrency = 1
	}
	if opt.DownloadConcurrency < 1 {
		opt.DownloadConcurrency = 1
	}
	if int64(opt.ThreadSize) < chunkSize*attachmentsPerMessage {
		return nil, fmt.Errorf("thread_size must be at least %v", fs.SizeSuffix(chunkSize*attachmentsPerMessage))
	}

	db, err := pgxpool.New(ctx, opt.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}

	client := fshttp.NewClient(ctx)
	f := &Fs{
		name:        name,
		root:        root,
		opt:         *opt,
		db:          db,
		srv:         rest.NewClient(client).SetRoot(opt.APIURL).SetErrorHandler(errorHandler),
		cdn:         rest.NewClient(client),
		pacer:       fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		dataPacer:   fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		thread:      &threadState{},
		urls:        &urlCache{urls: map[string]cachedURL{}},
		cloudBroken: &atomic.Bool{},
		noURLTable:  &atomic.Bool{},
	}
	f.srv.SetHeader("Authorization", opt.DiscordToken)
	f.srv.SetHeader("User-Agent", userAgent)
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		DuplicateFiles:          true,
		ReadMimeType:            true,
		WriteMimeType:           true,
	}).Fill(ctx, f)

	f.dirCache = dircache.New(root, rootID, f)

	// Find the current root
	err = f.dirCache.FindRoot(ctx, false)
	if err != nil {
		// Assume it is a file
		newRoot, remote := dircache.SplitPath(root)
		tempF := *f
		tempF.dirCache = dircache.New(newRoot, rootID, &tempF)
		tempF.root = newRoot
		// Make new Fs which is the parent
		err = tempF.dirCache.FindRoot(ctx, false)
		if err != nil {
			// No root so return old f
			return f, nil
		}
		_, err := tempF.newObjectWithInfo(ctx, remote, nil)
		if err != nil {
			if err == fs.ErrorObjectNotFound {
				// File doesn't exist so return old f
				return f, nil
			}
			return nil, err
		}
		f.features.Fill(ctx, &tempF)
		// XXX: update the old f here instead of returning tempF, since
		// `features` were already filled with functions having *f as a receiver.
		// See https://github.com/rclone/rclone/issues/2182
		f.dirCache = tempF.dirCache
		f.root = tempF.root
		// return an error with an fs which points to the parent
		return f, fs.ErrorIsFile
	}
	return f, nil
}

// FindLeaf finds a directory of name leaf in the folder with ID pathID
func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (pathIDOut string, found bool, err error) {
	return f.findDirID(ctx, pathID, f.opt.Enc.FromStandardName(leaf))
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, pathID, leaf string) (newID string, err error) {
	return f.insertDir(ctx, pathID, f.opt.Enc.FromStandardName(leaf))
}

// List the objects and directories in dir into entries. The entries
// can be returned in any order but should be for a complete directory.
//
// dir should be "" to list the root, and should not have trailing
// slashes.
//
// This should return ErrDirNotFound if the directory isn't found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}

	dirs, err := f.listDirs(ctx, dirID)
	if err != nil {
		return nil, err
	}
	for _, d := range dirs {
		remote := path.Join(dir, f.opt.Enc.ToStandardName(d.Name))
		f.dirCache.Put(remote, d.ID)
		entries = append(entries, fs.NewDir(remote, d.CreatedAt).SetID(d.ID))
	}

	files, err := f.listFiles(ctx, dirID)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		remote := path.Join(dir, f.opt.Enc.ToStandardName(file.Name))
		o, err := f.newObjectWithInfo(ctx, remote, file)
		if err != nil {
			return nil, err
		}
		entries = append(entries, o)
	}
	return entries, nil
}

// newObjectWithInfo creates an Object from file, or by looking up
// remote if file is nil
func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, file *dbFile) (*Object, error) {
	if file == nil {
		leaf, dirID, err := f.dirCache.FindPath(ctx, remote, false)
		if err != nil {
			if err == fs.ErrorDirNotFound {
				return nil, fs.ErrorObjectNotFound
			}
			return nil, err
		}
		file, err = f.findFile(ctx, dirID, f.opt.Enc.FromStandardName(leaf))
		if err != nil {
			return nil, err
		}
		if file == nil {
			return nil, fs.ErrorObjectNotFound
		}
	}
	o := &Object{fs: f, remote: remote}
	o.setMetaData(file)
	return o, nil
}

// NewObject finds the Object at remote. If it can't be found it
// returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return f.newObjectWithInfo(ctx, remote, nil)
}

// Put the object
//
// Copy the reader in to the new object which is returned.
//
// The new object may have been created if an error is returned
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	existing, err := f.NewObject(ctx, src.Remote())
	switch err {
	case nil:
		return existing, existing.Update(ctx, in, src, options...)
	case fs.ErrorObjectNotFound:
		o := &Object{fs: f, remote: src.Remote()}
		return o, o.Update(ctx, in, src, options...)
	default:
		return nil, err
	}
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(ctx, in, src, options...)
}

// Mkdir creates the directory if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// Rmdir deletes the directory if empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	if dirID == rootID {
		return errors.New("can't remove the dfs root")
	}
	empty, err := f.dirIsEmpty(ctx, dirID)
	if err != nil {
		return err
	}
	if !empty {
		return fs.ErrorDirectoryNotEmpty
	}
	if err := f.deleteDir(ctx, dirID); err != nil {
		return err
	}
	f.dirCache.FlushDir(dir)
	return nil
}

// Purge deletes all the files and directories in dir, including dir
// itself. Chunks stay in Discord; only the metadata is (soft) deleted.
func (f *Fs) Purge(ctx context.Context, dir string) error {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	if err := f.purgeDir(ctx, dirID); err != nil {
		return err
	}
	f.dirCache.FlushDir(dir)
	if dir == "" {
		f.dirCache.ResetRoot()
	}
	return nil
}

// sameStore reports whether src lives in the same database as f, so
// rows can be moved or copied between them
func (f *Fs) sameStore(src *Fs) bool {
	return f.opt.DatabaseURL == src.opt.DatabaseURL
}

// Copy src to this remote using server-side copy operations. The new
// row shares src's chunks, which are never deleted from Discord.
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok || !f.sameStore(srcObj.fs) {
		fs.Debugf(src, "Can't copy - not same remote type")
		return nil, fs.ErrorCantCopy
	}
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	srcFile, err := srcObj.fs.findFileByID(ctx, srcObj)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	file := *srcFile
	file.ID = uuid.NewString()
	file.ParentID = dirID
	file.Name = f.opt.Enc.FromStandardName(leaf)
	file.CreatedAt = now
	if err := f.replaceFile(ctx, &file); err != nil {
		return nil, err
	}
	return f.newObjectWithInfo(ctx, remote, &file)
}

// findFileByID returns the current row for o
func (f *Fs) findFileByID(ctx context.Context, o *Object) (*dbFile, error) {
	file, err := scanFile(f.db.QueryRow(ctx, `select `+fileColumns+` from files where id = $1 and deleted_at is null`, o.id))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", o.remote, err)
	}
	return file, nil
}

// Move src to this remote using server-side move operations.
//
// If it isn't possible then return fs.ErrorCantMove
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok || !f.sameStore(srcObj.fs) {
		fs.Debugf(src, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	if err := f.moveFile(ctx, srcObj.id, dirID, f.opt.Enc.FromStandardName(leaf)); err != nil {
		return nil, err
	}
	dstObj := *srcObj
	dstObj.fs = f
	dstObj.remote = remote
	return &dstObj, nil
}

// DirMove moves src, srcRemote to this remote at dstRemote using
// server-side move operations.
//
// If it isn't possible then return fs.ErrorCantDirMove
//
// If destination exists then return fs.ErrorDirExists
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok || !f.sameStore(srcFs) {
		fs.Debugf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}
	srcID, _, _, dstDirectoryID, dstLeaf, err := f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		return err
	}
	if err := f.moveDir(ctx, srcID, dstDirectoryID, f.opt.Enc.FromStandardName(dstLeaf)); err != nil {
		return err
	}
	srcFs.dirCache.FlushDir(srcRemote)
	return nil
}

// DirCacheFlush resets the directory cache - used in testing as an
// optional interface
func (f *Fs) DirCacheFlush() {
	f.dirCache.ResetRoot()
}

// About gets quota information
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	used, objects, err := f.usage(ctx)
	if err != nil {
		return nil, err
	}
	return &fs.Usage{Used: fs.NewUsageValue(used), Objects: fs.NewUsageValue(objects)}, nil
}

// Shutdown archives the active thread and closes the database pool
func (f *Fs) Shutdown(ctx context.Context) error {
	f.thread.mu.Lock()
	threadID := f.thread.id
	f.thread.id = ""
	f.thread.mu.Unlock()
	if threadID != "" {
		f.archiveThread(threadID)
	}
	f.db.Close()
	return nil
}

// ------------------------------------------------------------

func (o *Object) setMetaData(file *dbFile) {
	o.id = file.ID
	o.size = file.Size
	o.modTime = file.ModifiedAt
	o.mimeType = file.Type
	o.chunks = file.Chunks
	o.key = file.EncryptionKey
}

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// String returns a description of the Object
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// Hash is unsupported on dfs
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.size
}

// ModTime returns the modification time of the object
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// SetModTime sets the modification time of the object
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	modTime = modTime.Truncate(time.Microsecond)
	if err := o.fs.setFileModTime(ctx, o.id, modTime); err != nil {
		return err
	}
	o.modTime = modTime
	return nil
}

// Storable returns a boolean showing whether this object storable
func (o *Object) Storable() bool {
	return true
}

// MimeType of an Object if known, "" otherwise
func (o *Object) MimeType(ctx context.Context) string {
	return o.mimeType
}

// ID returns the ID of the Object if known, or "" if not
func (o *Object) ID() string {
	return o.id
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	var offset, limit int64 = 0, -1
	fs.FixRangeOption(options, o.size)
	for _, option := range options {
		switch x := option.(type) {
		case *fs.SeekOption:
			offset = x.Offset
		case *fs.RangeOption:
			offset, limit = x.Decode(o.size)
		default:
			if option.Mandatory() {
				fs.Logf(o, "Unsupported mandatory option: %v", option)
			}
		}
	}
	return o.newChunkReader(ctx, offset, limit)
}

// Update the object with the contents of the io.Reader, modTime and size
//
// If existing is set then it updates the object rather than creating a new one.
//
// The new object may have been created if an error is returned
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	f := o.fs
	leaf, dirID, err := f.dirCache.FindPath(ctx, o.remote, true)
	if err != nil {
		return err
	}

	// CTR with a fixed IV means a key must never be reused for new content
	encrypted := o.key != nil
	if !encrypted {
		encrypted, err = f.dirEncrypted(ctx, dirID)
		if err != nil {
			return err
		}
	}
	var key []byte
	if encrypted {
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return err
		}
	}

	chunks, hashes, written, err := f.uploadChunks(ctx, in, src.Size(), key)
	if err != nil {
		return err
	}

	file := &dbFile{
		ID:            o.id,
		ParentID:      dirID,
		Name:          f.opt.Enc.FromStandardName(leaf),
		Type:          fs.MimeType(ctx, src),
		Size:          written,
		CreatedAt:     time.Now(),
		ModifiedAt:    src.ModTime(ctx).Truncate(time.Microsecond),
		Chunks:        chunks,
		Hashes:        hashes,
		EncryptionKey: key,
	}
	if o.id == "" {
		file.ID = uuid.NewString()
		err = f.replaceFile(ctx, file)
	} else {
		err = f.updateFileContent(ctx, file)
	}
	if err != nil {
		return err
	}
	o.setMetaData(file)
	return nil
}

// Remove an object (soft delete; chunks stay in Discord)
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.deleteFile(ctx, o.id)
}

// Check the interfaces are satisfied
var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Purger          = (*Fs)(nil)
	_ fs.PutStreamer     = (*Fs)(nil)
	_ fs.Copier          = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.DirCacheFlusher = (*Fs)(nil)
	_ fs.Abouter         = (*Fs)(nil)
	_ fs.Shutdowner      = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.MimeTyper       = (*Object)(nil)
	_ fs.IDer            = (*Object)(nil)
)

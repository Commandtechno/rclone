package dfs

// Metadata lives in the same postgres tables the dfs tools (fsexp,
// fs-new) use. Deletes are soft (deleted_at) to match gorm.

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rclone/rclone/fs"
)

// rootID is the parent_id of top level entries (gorm writes uuid.Nil)
const rootID = "00000000-0000-0000-0000-000000000000"

type dbDir struct {
	ID        string
	Name      string
	CreatedAt time.Time
}

type dbFile struct {
	ID            string
	ParentID      string
	Name          string
	Type          string
	Size          int64
	CreatedAt     time.Time
	ModifiedAt    time.Time
	Chunks        []string // "channelID/attachmentID"
	Hashes        []string // md5 of each stored chunk
	EncryptionKey []byte
}

const fileColumns = `id::text, parent_id::text, name, coalesce(type, ''), coalesce(size, 0),
	coalesce(created_at, 'epoch'), coalesce(modified_at, created_at, 'epoch'),
	coalesce(chunks, ''), coalesce(hashes, ''), encryption_key`

// chunks and hashes are stored comma joined in a text column
func splitList(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

func joinList(l []string) string {
	return strings.Join(l, ",")
}

func scanFile(row pgx.Row) (*dbFile, error) {
	var file dbFile
	var chunks, hashes string
	err := row.Scan(&file.ID, &file.ParentID, &file.Name, &file.Type, &file.Size,
		&file.CreatedAt, &file.ModifiedAt, &chunks, &hashes, &file.EncryptionKey)
	if err != nil {
		return nil, err
	}
	file.Chunks = splitList(chunks)
	file.Hashes = splitList(hashes)
	return &file, nil
}

// findDirID returns the id of the directory called name in parentID
func (f *Fs) findDirID(ctx context.Context, parentID, name string) (id string, found bool, err error) {
	err = f.db.QueryRow(ctx, `select id::text from directories
		where parent_id = $1 and name = $2 and deleted_at is null
		order by id limit 1`, parentID, name).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return id, true, nil
}

func (f *Fs) insertDir(ctx context.Context, parentID, name string) (id string, err error) {
	if name == "" {
		return "", errors.New("refusing to create a directory with an empty name")
	}
	id = uuid.NewString()
	_, err = f.db.Exec(ctx, `insert into directories (id, parent_id, name, created_at, encrypted)
		values ($1, $2, $3, $4, false)`, id, parentID, name, time.Now())
	return id, err
}

func (f *Fs) listDirs(ctx context.Context, parentID string) ([]dbDir, error) {
	rows, err := f.db.Query(ctx, `select id::text, name, coalesce(created_at, 'epoch') from directories
		where parent_id = $1 and deleted_at is null`, parentID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (dbDir, error) {
		var d dbDir
		err := row.Scan(&d.ID, &d.Name, &d.CreatedAt)
		return d, err
	})
}

func (f *Fs) listFiles(ctx context.Context, parentID string) ([]*dbFile, error) {
	rows, err := f.db.Query(ctx, `select `+fileColumns+` from files
		where parent_id = $1 and deleted_at is null`, parentID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*dbFile, error) {
		return scanFile(row)
	})
}

// findFile returns nil if there is no file called name in parentID
func (f *Fs) findFile(ctx context.Context, parentID, name string) (*dbFile, error) {
	file, err := scanFile(f.db.QueryRow(ctx, `select `+fileColumns+` from files
		where parent_id = $1 and name = $2 and deleted_at is null
		order by id limit 1`, parentID, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return file, err
}

// replaceFile inserts file, soft deleting anything else with its name
func (f *Fs) replaceFile(ctx context.Context, file *dbFile) error {
	return pgx.BeginFunc(ctx, f.db, func(tx pgx.Tx) error {
		if err := softDeleteNamesake(ctx, tx, file.ParentID, file.Name, file.ID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `insert into files
			(id, parent_id, name, type, size, chunks, hashes, encryption_key, created_at, modified_at)
			values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			file.ID, file.ParentID, file.Name, file.Type, file.Size,
			joinList(file.Chunks), joinList(file.Hashes), file.EncryptionKey,
			file.CreatedAt, file.ModifiedAt)
		return err
	})
}

// updateFileContent points an existing file at newly uploaded chunks
func (f *Fs) updateFileContent(ctx context.Context, file *dbFile) error {
	_, err := f.db.Exec(ctx, `update files set
		type = $2, size = $3, chunks = $4, hashes = $5, encryption_key = $6, modified_at = $7
		where id = $1`,
		file.ID, file.Type, file.Size, joinList(file.Chunks), joinList(file.Hashes),
		file.EncryptionKey, file.ModifiedAt)
	return err
}

func (f *Fs) setFileModTime(ctx context.Context, id string, modTime time.Time) error {
	_, err := f.db.Exec(ctx, `update files set modified_at = $2 where id = $1`, id, modTime)
	return err
}

// moveFile renames/reparents a file, replacing anything at the destination
func (f *Fs) moveFile(ctx context.Context, id, parentID, name string) error {
	return pgx.BeginFunc(ctx, f.db, func(tx pgx.Tx) error {
		if err := softDeleteNamesake(ctx, tx, parentID, name, id); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `update files set parent_id = $2, name = $3 where id = $1`, id, parentID, name)
		return err
	})
}

func softDeleteNamesake(ctx context.Context, tx pgx.Tx, parentID, name, keepID string) error {
	_, err := tx.Exec(ctx, `update files set deleted_at = $4
		where parent_id = $1 and name = $2 and id <> $3 and deleted_at is null`,
		parentID, name, keepID, time.Now())
	return err
}

func (f *Fs) moveDir(ctx context.Context, id, parentID, name string) error {
	_, err := f.db.Exec(ctx, `update directories set parent_id = $2, name = $3 where id = $1`, id, parentID, name)
	return err
}

func (f *Fs) deleteFile(ctx context.Context, id string) error {
	_, err := f.db.Exec(ctx, `update files set deleted_at = $2 where id = $1`, id, time.Now())
	return err
}

func (f *Fs) dirIsEmpty(ctx context.Context, id string) (bool, error) {
	var nonEmpty bool
	err := f.db.QueryRow(ctx, `select
		exists (select 1 from directories where parent_id = $1 and deleted_at is null) or
		exists (select 1 from files where parent_id = $1 and deleted_at is null)`, id).Scan(&nonEmpty)
	return !nonEmpty, err
}

func (f *Fs) deleteDir(ctx context.Context, id string) error {
	_, err := f.db.Exec(ctx, `update directories set deleted_at = $2 where id = $1`, id, time.Now())
	return err
}

// purgeDir soft deletes id and everything beneath it
func (f *Fs) purgeDir(ctx context.Context, id string) error {
	// seeding with id's children as well as id itself means purging the
	// root (which has no row) works too
	_, err := f.db.Exec(ctx, `with recursive tree as (
			select id from directories where (id = $1 or parent_id = $1) and deleted_at is null
			union
			select d.id from directories d join tree on d.parent_id = tree.id
			where d.deleted_at is null
		), deleted_files as (
			update files set deleted_at = $2
			where (parent_id = $1 or parent_id in (select id from tree)) and deleted_at is null
		) update directories set deleted_at = $2 where id in (select id from tree)`, id, time.Now())
	return err
}

// dirEncrypted reports whether id or any of its ancestors is marked encrypted
func (f *Fs) dirEncrypted(ctx context.Context, id string) (encrypted bool, err error) {
	if id == rootID {
		return false, nil
	}
	err = f.db.QueryRow(ctx, `with recursive up as (
			select id, parent_id, encrypted from directories where id = $1
			union
			select d.id, d.parent_id, d.encrypted from directories d join up on d.id = up.parent_id
		) select coalesce(bool_or(encrypted), false) from up`, id).Scan(&encrypted)
	return encrypted, err
}

func (f *Fs) usage(ctx context.Context) (used, objects int64, err error) {
	err = f.db.QueryRow(ctx, `select coalesce(sum(size), 0)::bigint, count(*) from files where deleted_at is null`).Scan(&used, &objects)
	return used, objects, err
}

// The chunk_urls table is created by `fs setup`. If it's missing we
// just run with the in-memory cache.

type chunkURL struct {
	AttachmentID string
	ChannelID    string
	URL          string
	ExpiresAt    time.Time
}

func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

func (f *Fs) loadChunkURL(ctx context.Context, attachmentID string) (url string, expires time.Time, ok bool) {
	if f.noURLTable.Load() {
		return "", time.Time{}, false
	}
	err := f.db.QueryRow(ctx, `select url, expires_at from chunk_urls where attachment_id = $1`, attachmentID).Scan(&url, &expires)
	if err != nil {
		if isUndefinedTable(err) {
			f.noURLTable.Store(true)
		} else if !errors.Is(err, pgx.ErrNoRows) {
			fs.Debugf(f, "url cache lookup failed: %v", err)
		}
		return "", time.Time{}, false
	}
	return url, expires, true
}

func (f *Fs) saveChunkURLs(ctx context.Context, rows []chunkURL) {
	if len(rows) == 0 || f.noURLTable.Load() {
		return
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(`insert into chunk_urls (attachment_id, channel_id, url, expires_at) values ($1, $2, $3, $4)
			on conflict (attachment_id) do update set channel_id = excluded.channel_id, url = excluded.url, expires_at = excluded.expires_at`,
			row.AttachmentID, row.ChannelID, row.URL, row.ExpiresAt)
	}
	if err := f.db.SendBatch(ctx, batch).Close(); err != nil {
		if isUndefinedTable(err) {
			f.noURLTable.Store(true)
			return
		}
		fs.Debugf(f, "failed to persist url cache: %v", err)
	}
}

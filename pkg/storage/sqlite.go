package storage

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var _ = (Storage)((*sqliteStorage)(nil))

type sqliteStorage struct {
	db *sql.DB
}

// Should be closed after being used
func NewSqlite(dbPath string) (*sqliteStorage, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_synchronous=off&_journal_mode=off&cache=shared")
	if err != nil {
		return nil, err
	}
	if _, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS files (
			path TEXT PRIMARY KEY,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			accessed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`); err != nil {
		return nil, err
	}

	storage = &sqliteStorage{
		db: db,
	}

	return storage.(*sqliteStorage), nil
}

func (s *sqliteStorage) InsertPath(ctx context.Context, path string, updatedAt time.Time) (*time.Time, error) {
	if _, err := s.db.ExecContext(ctx, "INSERT OR IGNORE INTO files (path, updated_at) VALUES (?, ?)", path, updatedAt); err != nil {
		return nil, err
	}

	timestamp := new(time.Time)
	row := s.db.QueryRowContext(ctx, "SELECT updated_at FROM files WHERE path = ?", path)
	err := row.Scan(timestamp)
	return timestamp, err
}

func (s *sqliteStorage) UpdateAccess(ctx context.Context, path string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE files SET accessed_at = DATETIME() WHERE path = ?", path)
	return err
}

func (s *sqliteStorage) Close() error {
	return s.db.Close()
}

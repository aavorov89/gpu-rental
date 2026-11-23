// Package testdb gives a test binary its own migrated Postgres database.
// The database has a random name and is dropped by the returned cleanup, so
// tests never read or write the development tables.
package testdb

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

const defaultURL = "postgres://gpu_rental:gpu_rental@127.0.0.1:5432/gpu_rental?sslmode=disable"

// Open creates and migrates a temporary database. TEST_DATABASE_URL selects the
// server; its role must be allowed to create databases.
func Open(ctx context.Context) (*sql.DB, func(), error) {
	serverURL := os.Getenv("TEST_DATABASE_URL")
	if serverURL == "" {
		serverURL = defaultURL
	}

	parsed, err := url.Parse(serverURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse TEST_DATABASE_URL: %w", err)
	}

	admin, err := sql.Open("pgx", serverURL)
	if err != nil {
		return nil, nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := admin.PingContext(pingCtx); err != nil {
		admin.Close()
		return nil, nil, fmt.Errorf("connect to Postgres (run `docker compose up -d --wait` or set TEST_DATABASE_URL): %w", err)
	}

	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		admin.Close()
		return nil, nil, err
	}

	// The name contains only a fixed prefix and hex, so it is safe to inline.
	name := "gpu_rental_test_" + hex.EncodeToString(suffix)

	if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+name); err != nil {
		admin.Close()
		return nil, nil, fmt.Errorf("create test database: %w", err)
	}

	dropDatabase := func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()

		if _, err := admin.ExecContext(dropCtx, `DROP DATABASE IF EXISTS `+name+` WITH (FORCE)`); err != nil {
			fmt.Fprintf(os.Stderr, "drop test database %s: %v\n", name, err)
		}
		admin.Close()
	}

	testURL := *parsed
	testURL.Path = "/" + name

	db, err := sql.Open("pgx", testURL.String())
	if err != nil {
		dropDatabase()
		return nil, nil, err
	}

	if err := migrate(ctx, db); err != nil {
		db.Close()
		dropDatabase()
		return nil, nil, err
	}

	cleanup := func() {
		db.Close()
		dropDatabase()
	}

	return db, cleanup, nil
}

func migrate(ctx context.Context, db *sql.DB) error {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("locate migrations directory")
	}

	dir := filepath.Join(filepath.Dir(file), "..", "..", "migrations")

	provider, err := goose.NewProvider(goose.DialectPostgres, db, os.DirFS(dir))
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}

	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	return nil
}

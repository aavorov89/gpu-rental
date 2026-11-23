// Package testdb gives a test binary its own migrated Postgres database.
// The database has a random name and is dropped by the returned cleanup, so
// tests never read or write the development tables.
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

const (
	defaultURL = "postgres://gpu_rental:gpu_rental@127.0.0.1:5432/gpu_rental?sslmode=disable"
	maxConns   = 20
)

// Open creates and migrates a temporary database. TEST_DATABASE_URL selects the
// server; its role must be allowed to create databases.
func Open(ctx context.Context) (*pgxpool.Pool, func(), error) {
	serverURL := os.Getenv("TEST_DATABASE_URL")
	if serverURL == "" {
		serverURL = defaultURL
	}

	config, err := pgxpool.ParseConfig(serverURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse TEST_DATABASE_URL: %w", err)
	}

	connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	admin, err := pgx.Connect(connectCtx, serverURL)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to Postgres (run `docker compose up -d --wait` or set TEST_DATABASE_URL): %w", err)
	}

	closeAdmin := func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()

		admin.Close(closeCtx)
	}

	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		closeAdmin()
		return nil, nil, err
	}

	// The name contains only a fixed prefix and hex, so it is safe to inline.
	name := "gpu_rental_test_" + hex.EncodeToString(suffix)

	if _, err := admin.Exec(ctx, `CREATE DATABASE `+name); err != nil {
		closeAdmin()
		return nil, nil, fmt.Errorf("create test database: %w", err)
	}

	dropDatabase := func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()

		if _, err := admin.Exec(dropCtx, `DROP DATABASE IF EXISTS `+name+` WITH (FORCE)`); err != nil {
			fmt.Fprintf(os.Stderr, "drop test database %s: %v\n", name, err)
		}
		closeAdmin()
	}

	config.ConnConfig.Database = name
	config.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		dropDatabase()
		return nil, nil, fmt.Errorf("open test pool: %w", err)
	}

	if err := migrate(ctx, pool); err != nil {
		pool.Close()
		dropDatabase()
		return nil, nil, err
	}

	cleanup := func() {
		pool.Close()
		dropDatabase()
	}

	return pool, cleanup, nil
}

func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("locate migrations directory")
	}

	dir := filepath.Join(filepath.Dir(file), "..", "..", "migrations")

	// Goose needs database/sql, so only migrations go through this adapter.
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()

	provider, err := goose.NewProvider(goose.DialectPostgres, db, os.DirFS(dir))
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}

	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	return nil
}

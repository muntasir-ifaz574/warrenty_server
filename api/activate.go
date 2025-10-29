package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type activateRequest struct {
	Brand         string    `json:"brand"`
	Model         string    `json:"model"`
	SerialNumber  string    `json:"serialNumber"`
	DeviceID      string    `json:"deviceId"`
	ActivatedAt   time.Time `json:"activatedAt"`
	ActiveMinutes int       `json:"activeMinutes"`
}

type activateResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

var (
	db          *sql.DB
	dbInitError error
)

func init() {
	// Initialize DB on cold start. If DATABASE_URL is not provided, keep db nil.
	dsn := os.Getenv("DATABASE_URL")
	if strings.TrimSpace(dsn) == "" {
		dbInitError = errors.New("DATABASE_URL not set; persistence disabled")
		return
	}

	// pgx/v5 stdlib uses "pgx" driver name
	conn, err := sql.Open("pgx", dsn)
	if err != nil {
		dbInitError = fmt.Errorf("open db: %w", err)
		return
	}

	// Reasonable limits for serverless
	conn.SetMaxOpenConns(3)
	conn.SetMaxIdleConns(3)
	conn.SetConnMaxIdleTime(2 * time.Minute)
	conn.SetConnMaxLifetime(10 * time.Minute)

	// Ensure connection works
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		dbInitError = fmt.Errorf("ping db: %w", err)
		_ = conn.Close()
		return
	}

	// Auto-migrate table
	if err := migrate(ctx, conn); err != nil {
		dbInitError = fmt.Errorf("migrate: %w", err)
		_ = conn.Close()
		return
	}

	db = conn
}

func migrate(ctx context.Context, conn *sql.DB) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS activations (
    id BIGSERIAL PRIMARY KEY,
    brand TEXT NOT NULL,
    model TEXT NOT NULL,
    serial_number TEXT NOT NULL UNIQUE,
    device_id TEXT NOT NULL,
    activated_at TIMESTAMPTZ NOT NULL,
    active_minutes INTEGER NOT NULL CHECK (active_minutes >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS activations_set_updated_at ON activations;
CREATE TRIGGER activations_set_updated_at
BEFORE UPDATE ON activations
FOR EACH ROW EXECUTE FUNCTION set_updated_at();
`
	_, err := conn.ExecContext(ctx, ddl)
	return err
}

// Handler is the Vercel entrypoint.
func Handler(w http.ResponseWriter, r *http.Request) {
	// CORS: allow POST from Android TV app
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req activateRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	if err := validate(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Persist if DB configured; otherwise accept but warn in response message
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	persisted := false
	if db != nil && dbInitError == nil {
		if err := upsertActivation(ctx, db, req); err != nil {
			// Do not expose internals; return 500
			http.Error(w, "failed to store activation", http.StatusInternalServerError)
			return
		}
		persisted = true
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	resp := activateResponse{Status: "ok"}
	if persisted {
		resp.Message = "activation stored"
	} else {
		resp.Message = "accepted without persistence (DATABASE_URL not set)"
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func validate(a activateRequest) error {
	if strings.TrimSpace(a.Brand) == "" {
		return errors.New("brand is required")
	}
	if strings.TrimSpace(a.Model) == "" {
		return errors.New("model is required")
	}
	if strings.TrimSpace(a.SerialNumber) == "" {
		return errors.New("serialNumber is required")
	}
	if strings.TrimSpace(a.DeviceID) == "" {
		return errors.New("deviceId is required")
	}
	if a.ActiveMinutes < 0 {
		return errors.New("activeMinutes must be >= 0")
	}
	if a.ActivatedAt.IsZero() {
		return errors.New("activatedAt is required (UTC ISO-8601)")
	}
	return nil
}

func upsertActivation(ctx context.Context, conn *sql.DB, a activateRequest) error {
	const q = `
INSERT INTO activations (
  brand, model, serial_number, device_id, activated_at, active_minutes
) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (serial_number) DO UPDATE SET
  brand = EXCLUDED.brand,
  model = EXCLUDED.model,
  device_id = EXCLUDED.device_id,
  activated_at = EXCLUDED.activated_at,
  active_minutes = EXCLUDED.active_minutes
`
	_, err := conn.ExecContext(
		ctx,
		q,
		a.Brand,
		a.Model,
		a.SerialNumber,
		a.DeviceID,
		a.ActivatedAt.UTC(),
		a.ActiveMinutes,
	)
	return err
}

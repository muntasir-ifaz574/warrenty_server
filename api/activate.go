package handler

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
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
    serial_number TEXT NOT NULL,
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

	-- Drop any existing unique constraint that applies only to (serial_number), regardless of its name
	DO $$
	DECLARE
	  cons RECORD;
	BEGIN
	  FOR cons IN (
	    SELECT c.conname
	    FROM pg_constraint c
	    JOIN pg_class t ON t.oid = c.conrelid
	    JOIN pg_namespace ns ON ns.oid = t.relnamespace
	    WHERE t.relname = 'activations'
	      AND c.contype = 'u'
	      AND (
	        SELECT array_agg(att.attname ORDER BY att.attnum)
	        FROM unnest(c.conkey) WITH ORDINALITY AS cols(attnum, ord)
	        JOIN pg_attribute att ON att.attrelid = t.oid AND att.attnum = cols.attnum
	      ) = ARRAY['serial_number']
	  ) LOOP
	    EXECUTE format('ALTER TABLE activations DROP CONSTRAINT %I', cons.conname);
	  END LOOP;
	END$$;

-- Enforce uniqueness on (serial_number, device_id)
CREATE UNIQUE INDEX IF NOT EXISTS activations_serial_device_uidx
ON activations (serial_number, device_id);
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
	ct := r.Header.Get("Content-Type")
	if strings.Contains(ct, "multipart/form-data") || strings.Contains(ct, "application/x-www-form-urlencoded") {
		// Accept form-data and x-www-form-urlencoded
		// Limit to 1MB form memory; no files expected
		if strings.Contains(ct, "multipart/form-data") {
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				http.Error(w, "invalid multipart form", http.StatusBadRequest)
				return
			}
		} else {
			if err := r.ParseForm(); err != nil {
				http.Error(w, "invalid form body", http.StatusBadRequest)
				return
			}
		}
		req.Brand = strings.TrimSpace(r.FormValue("brand"))
		req.Model = strings.TrimSpace(r.FormValue("model"))
		req.SerialNumber = strings.TrimSpace(r.FormValue("serialNumber"))
		req.DeviceID = strings.TrimSpace(r.FormValue("deviceId"))
		// Parse activatedAt as RFC3339
		activatedAtStr := strings.TrimSpace(r.FormValue("activatedAt"))
		if activatedAtStr != "" {
			t, err := time.Parse(time.RFC3339, activatedAtStr)
			if err != nil {
				http.Error(w, "activatedAt must be RFC3339 (e.g., 2025-10-29T12:34:56Z)", http.StatusBadRequest)
				return
			}
			req.ActivatedAt = t
		}
		// Parse activeMinutes
		if am := strings.TrimSpace(r.FormValue("activeMinutes")); am != "" {
			v, err := strconv.Atoi(am)
			if err != nil {
				http.Error(w, "activeMinutes must be an integer", http.StatusBadRequest)
				return
			}
			req.ActiveMinutes = v
		}
	} else {
		// Default to JSON
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
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
			http.Error(w, "failed to store activation: "+err.Error(), http.StatusInternalServerError)
			return
		}
		persisted = true
	} else {
		// Try Supabase REST fallback if DATABASE_URL is not configured
		if sbURL := strings.TrimSpace(os.Getenv("SUPABASE_URL")); sbURL != "" {
			if err := upsertActivationSupabase(ctx, sbURL, os.Getenv("SUPABASE_SERVICE_ROLE_KEY"), req); err != nil {
				http.Error(w, "failed to store activation: "+err.Error(), http.StatusInternalServerError)
				return
			}
			persisted = true
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	resp := activateResponse{Status: "ok"}
	if persisted {
		resp.Message = "activation stored"
	} else {
		resp.Message = "accepted without persistence (no DATABASE_URL or Supabase configured)"
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
ON CONFLICT (serial_number, device_id) DO UPDATE SET
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

func upsertActivationSupabase(ctx context.Context, supabaseURL, serviceRoleKey string, a activateRequest) error {
	if strings.TrimSpace(supabaseURL) == "" || strings.TrimSpace(serviceRoleKey) == "" {
		return errors.New("supabase not configured")
	}
	// PostgREST upsert with on_conflict on (serial_number, device_id)
	// Endpoint: {SUPABASE_URL}/rest/v1/activations?on_conflict=serial_number,device_id
	endpoint := strings.TrimRight(supabaseURL, "/") + "/rest/v1/activations?on_conflict=serial_number,device_id"

	payload := map[string]any{
		"brand":          a.Brand,
		"model":          a.Model,
		"serial_number":  a.SerialNumber,
		"device_id":      a.DeviceID,
		"activated_at":   a.ActivatedAt.UTC().Format(time.RFC3339),
		"active_minutes": a.ActiveMinutes,
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("apikey", serviceRoleKey)
	req.Header.Set("Authorization", "Bearer "+serviceRoleKey)
	// Upsert behavior
	req.Header.Set("Prefer", "resolution=merge-duplicates")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		// include response body for diagnostics
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("supabase insert failed: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}

-- Create note_history table for historical note snapshots
CREATE TABLE IF NOT EXISTS note_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    note_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    tags TEXT DEFAULT '[]',
    version INTEGER NOT NULL,
    device_id TEXT,
    updated_at TEXT NOT NULL,
    snapshot_at TEXT NOT NULL,
    FOREIGN KEY (note_id) REFERENCES notes(id) ON DELETE CASCADE
);

-- Index for per-note history lookups (newest first)
CREATE INDEX IF NOT EXISTS idx_note_history_note ON note_history(note_id, id);

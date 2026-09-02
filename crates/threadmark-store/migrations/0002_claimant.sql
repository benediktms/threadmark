UPDATE nodes
SET lifecycle = 'open',
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE lifecycle = 'in_progress'
  AND id IN (SELECT node_id FROM claims WHERE released_at IS NULL);

UPDATE claims
SET released_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
    release_reason = 'claimant migration'
WHERE released_at IS NULL;

ALTER TABLE claims RENAME COLUMN session_id TO claimant;

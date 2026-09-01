CREATE TABLE workspaces (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    root_uri TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE efforts (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    slug TEXT NOT NULL,
    title TEXT NOT NULL,
    destination TEXT NOT NULL,
    scope_notes TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(workspace_id, slug)
);

CREATE TABLE nodes (
    id TEXT PRIMARY KEY,
    effort_id TEXT NOT NULL REFERENCES efforts(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT NOT NULL DEFAULT '',
    lifecycle TEXT NOT NULL,
    validity TEXT NOT NULL,
    confidence TEXT,
    confidence_reason TEXT,
    reversibility TEXT,
    impact TEXT,
    uncertainty TEXT,
    cost_of_wrong TEXT,
    current_revision INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE node_revisions (
    node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL,
    body TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL,
    reason TEXT,
    actor_id TEXT NOT NULL,
    session_id TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY(node_id, revision)
);

CREATE TABLE edges (
    id TEXT PRIMARY KEY,
    effort_id TEXT NOT NULL REFERENCES efforts(id) ON DELETE CASCADE,
    source_node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    target_node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    rationale TEXT,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(source_node_id, type, target_node_id),
    CHECK(source_node_id <> target_node_id)
);

CREATE TABLE sources (
    id TEXT PRIMARY KEY,
    effort_id TEXT NOT NULL REFERENCES efforts(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    uri TEXT,
    title TEXT NOT NULL,
    retrieved_at TEXT,
    observed_at TEXT,
    content_hash TEXT,
    excerpt TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    trust TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE node_sources (
    node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    relationship TEXT NOT NULL,
    PRIMARY KEY(node_id, source_id, relationship)
);

CREATE TABLE fog_patches (
    id TEXT PRIMARY KEY,
    effort_id TEXT NOT NULL REFERENCES efforts(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    anchor_node_id TEXT REFERENCES nodes(id) ON DELETE SET NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE fog_graduations (
    fog_id TEXT NOT NULL REFERENCES fog_patches(id) ON DELETE CASCADE,
    node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    PRIMARY KEY(fog_id, node_id)
);

CREATE TABLE claims (
    id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    actor_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    claimed_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    released_at TEXT,
    release_reason TEXT
);

CREATE UNIQUE INDEX claims_one_active_per_node
ON claims(node_id)
WHERE released_at IS NULL;

CREATE TABLE exit_criteria (
    id TEXT PRIMARY KEY,
    effort_id TEXT NOT NULL REFERENCES efforts(id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    config_json TEXT NOT NULL,
    required INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE findings (
    id TEXT PRIMARY KEY,
    effort_id TEXT NOT NULL REFERENCES efforts(id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT NOT NULL,
    title TEXT NOT NULL,
    detail TEXT NOT NULL,
    related_nodes_json TEXT NOT NULL,
    proposed_by TEXT,
    adjudication TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE events (
    id TEXT PRIMARY KEY,
    effort_id TEXT REFERENCES efforts(id) ON DELETE CASCADE,
    actor_id TEXT NOT NULL,
    session_id TEXT,
    event_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    before_json TEXT,
    after_json TEXT,
    reason TEXT,
    occurred_at TEXT NOT NULL
);

CREATE INDEX nodes_effort_state ON nodes(effort_id, kind, lifecycle, validity);
CREATE INDEX edges_source_type ON edges(source_node_id, type);
CREATE INDEX edges_target_type ON edges(target_node_id, type);
CREATE INDEX claims_node_expiry ON claims(node_id, lease_expires_at);
CREATE INDEX findings_effort_state ON findings(effort_id, status, severity);
CREATE INDEX events_effort_time ON events(effort_id, occurred_at);

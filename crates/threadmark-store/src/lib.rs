//! SQLite persistence for Threadmark.

use std::{path::Path, str::FromStr, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{
    QueryBuilder, Row, Sqlite, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow},
};
use thiserror::Error;
use threadmark_domain::{
    AuditEvent, Claim, Confidence, Edge, Effort, EventFilter, ExitCriterion, Finding, FogPatch,
    GraphSnapshot, Lifecycle, Node, NodeRevision, Reversibility, RiskLevel, Source, Uncertainty,
    Workspace,
};

#[derive(Clone, Copy, Debug)]
pub struct Pagination {
    pub limit: u32,
    pub offset: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EventCursor {
    pub occurred_at: String,
    pub id: String,
    pub through_rowid: i64,
    pub filter: EventFilter,
}

#[derive(Clone, Debug)]
pub struct EventPage {
    pub limit: u32,
    pub cursor: Option<EventCursor>,
}
use time::{
    Duration as TimeDuration, OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339,
};
use ulid::Ulid;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("database migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("invalid persisted value for {field}: {value}")]
    InvalidEnum { field: &'static str, value: String },
    #[error("invalid persisted JSON in {field}: {source}")]
    InvalidJson {
        field: &'static str,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid RFC 3339 timestamp: {0}")]
    InvalidTimestamp(String),
    #[error("active claim already exists for node {0}")]
    ClaimConflict(String),
    #[error("effort is not active")]
    EffortInactive,
    #[error("effort has active claims")]
    ActiveClaims,
    #[error("entity was not found")]
    NotFound,
    #[error("claim is not actively owned by {0}")]
    ClaimNotOwned(String),
    #[error("effort version conflict: expected {expected}, actual {actual}")]
    VersionConflict { expected: i64, actual: i64 },
    #[error("history cursor filters changed")]
    CursorFilterMismatch,
}

#[derive(Clone, Debug)]
pub struct Store {
    pool: SqlitePool,
}

#[derive(Clone, Copy, Debug)]
pub enum ClaimGuard<'a> {
    None,
    MustOwn(&'a str),
    OwnIfClaimed(&'a str),
}

impl Store {
    pub async fn connect(path: &Path) -> Result<Self, StoreError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(sqlx::Error::Io)?;
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(10)
            .connect_with(options)
            .await?;
        sqlx::migrate!().run(&pool).await?;
        normalize_event_timestamps(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn create_workspace(&self, workspace: &Workspace) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO workspaces(id,name,root_uri,schema_version,created_at,updated_at) VALUES(?,?,?,?,?,?)",
        )
        .bind(&workspace.id)
        .bind(&workspace.name)
        .bind(&workspace.root_uri)
        .bind(workspace.schema_version)
        .bind(&workspace.created_at)
        .bind(&workspace.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn reconcile_workspace(&self, workspace: &Workspace) -> Result<(), StoreError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let root_match = sqlx::query("SELECT * FROM workspaces WHERE root_uri = ?")
            .bind(&workspace.root_uri)
            .fetch_optional(&mut *tx)
            .await?
            .map(row_to_workspace);
        let target = sqlx::query("SELECT * FROM workspaces WHERE id = ?")
            .bind(&workspace.id)
            .fetch_optional(&mut *tx)
            .await?
            .map(row_to_workspace);

        if let Some(existing) = target {
            if existing.name != workspace.name
                || existing.root_uri != workspace.root_uri
                || existing.schema_version != workspace.schema_version
            {
                sqlx::query(
                    "UPDATE workspaces SET name=?,root_uri=?,schema_version=?,updated_at=? WHERE id=?",
                )
                .bind(&workspace.name)
                .bind(&workspace.root_uri)
                .bind(workspace.schema_version)
                .bind(&workspace.updated_at)
                .bind(&workspace.id)
                .execute(&mut *tx)
                .await?;
            }
        } else if let Some(existing) = root_match.as_ref() {
            let metadata_changed = existing.name != workspace.name
                || existing.root_uri != workspace.root_uri
                || existing.schema_version != workspace.schema_version;
            sqlx::query(
                "INSERT INTO workspaces(id,name,root_uri,schema_version,created_at,updated_at) VALUES(?,?,?,?,?,?)",
            )
            .bind(&workspace.id)
            .bind(&workspace.name)
            .bind(&workspace.root_uri)
            .bind(workspace.schema_version)
            .bind(&existing.created_at)
            .bind(if metadata_changed { &workspace.updated_at } else { &existing.updated_at })
            .execute(&mut *tx)
            .await?;
        } else {
            sqlx::query(
                "INSERT INTO workspaces(id,name,root_uri,schema_version,created_at,updated_at) VALUES(?,?,?,?,?,?)",
            )
            .bind(&workspace.id)
            .bind(&workspace.name)
            .bind(&workspace.root_uri)
            .bind(workspace.schema_version)
            .bind(&workspace.created_at)
            .bind(&workspace.updated_at)
            .execute(&mut *tx)
            .await?;
        }
        if let Some(existing) = root_match.filter(|existing| existing.id != workspace.id) {
            let conflicts = sqlx::query(
                "SELECT id,slug FROM efforts legacy WHERE workspace_id = ? \
                 AND EXISTS (SELECT 1 FROM efforts target WHERE target.workspace_id = ? AND target.slug = legacy.slug) \
                 ORDER BY id",
            )
            .bind(&existing.id)
            .bind(&workspace.id)
            .fetch_all(&mut *tx)
            .await?;
            for conflict in conflicts {
                let id: String = conflict.get("id");
                let slug: String = conflict.get("slug");
                let mut suffix = 1;
                loop {
                    let candidate = if suffix == 1 {
                        format!("{slug}-{id}")
                    } else {
                        format!("{slug}-{id}-{suffix}")
                    };
                    let exists: i64 = sqlx::query_scalar(
                        "SELECT COUNT(*) FROM efforts WHERE workspace_id IN (?, ?) AND slug = ?",
                    )
                    .bind(&existing.id)
                    .bind(&workspace.id)
                    .bind(&candidate)
                    .fetch_one(&mut *tx)
                    .await?;
                    if exists == 0 {
                        sqlx::query("UPDATE efforts SET slug = ? WHERE id = ?")
                            .bind(candidate)
                            .bind(id)
                            .execute(&mut *tx)
                            .await?;
                        break;
                    }
                    suffix += 1;
                }
            }
            sqlx::query("UPDATE efforts SET workspace_id = ? WHERE workspace_id = ?")
                .bind(&workspace.id)
                .bind(&existing.id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM workspaces WHERE id = ?")
                .bind(&existing.id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_workspace(&self, id: &str) -> Result<Workspace, StoreError> {
        let row = sqlx::query("SELECT * FROM workspaces WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(StoreError::NotFound)?;
        Ok(row_to_workspace(row))
    }

    pub async fn create_effort(
        &self,
        effort: &Effort,
        event: &AuditEvent,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO efforts(id,workspace_id,slug,title,destination,scope_notes,status,version,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)")
            .bind(&effort.id).bind(&effort.workspace_id).bind(&effort.slug).bind(&effort.title)
            .bind(&effort.destination).bind(&effort.scope_notes).bind(effort.status.as_str())
            .bind(effort.version).bind(&effort.created_at).bind(&effort.updated_at)
            .execute(&mut *tx).await?;
        insert_event(&mut tx, event).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn complete_effort(
        &self,
        effort: &Effort,
        event: &AuditEvent,
        expected_version: i64,
    ) -> Result<i64, StoreError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let cutoff = reap_expired_claims(&mut tx, &effort.id).await?;
        check_version(&mut tx, &effort.id, expected_version).await?;
        let result = sqlx::query(
            "UPDATE efforts SET status='completed' WHERE id=? AND status='active' \
             AND NOT EXISTS (SELECT 1 FROM claims JOIN nodes ON nodes.id=claims.node_id \
                             WHERE nodes.effort_id=? AND claims.released_at IS NULL \
                             AND claims.lease_expires_at>?)",
        )
        .bind(&effort.id)
        .bind(&effort.id)
        .bind(&cutoff)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 0 {
            let active_claims: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM claims JOIN nodes ON nodes.id=claims.node_id \
                 WHERE nodes.effort_id=? AND claims.released_at IS NULL \
                 AND claims.lease_expires_at>?",
            )
            .bind(&effort.id)
            .bind(&cutoff)
            .fetch_one(&mut *tx)
            .await?;
            return Err(if active_claims > 0 {
                StoreError::ActiveClaims
            } else {
                StoreError::EffortInactive
            });
        }
        insert_event(&mut tx, event).await?;
        let version = bump_version(&mut tx, &effort.id, &effort.updated_at).await?;
        tx.commit().await?;
        Ok(version)
    }

    pub async fn reopen_effort(
        &self,
        effort: &Effort,
        event: &AuditEvent,
        expected_version: i64,
    ) -> Result<i64, StoreError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row = sqlx::query("SELECT version,status FROM efforts WHERE id=?")
            .bind(&effort.id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(StoreError::NotFound)?;
        let actual: i64 = row.get("version");
        if actual != expected_version {
            return Err(StoreError::VersionConflict {
                expected: expected_version,
                actual,
            });
        }
        if row.get::<String, _>("status") != "completed" {
            return Err(StoreError::EffortInactive);
        }
        sqlx::query("UPDATE efforts SET status='active' WHERE id=?")
            .bind(&effort.id)
            .execute(&mut *tx)
            .await?;
        insert_event(&mut tx, event).await?;
        let version = bump_version(&mut tx, &effort.id, &effort.updated_at).await?;
        tx.commit().await?;
        Ok(version)
    }

    pub async fn list_efforts(&self, workspace_id: &str) -> Result<Vec<Effort>, StoreError> {
        let rows = sqlx::query("SELECT * FROM efforts WHERE workspace_id = ? ORDER BY created_at")
            .bind(workspace_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(row_to_effort).collect()
    }

    pub async fn get_effort(
        &self,
        workspace_id: &str,
        selector: &str,
    ) -> Result<Effort, StoreError> {
        let row =
            sqlx::query("SELECT * FROM efforts WHERE workspace_id = ? AND (id = ? OR slug = ?)")
                .bind(workspace_id)
                .bind(selector)
                .bind(selector)
                .fetch_optional(&self.pool)
                .await?
                .ok_or(StoreError::NotFound)?;
        row_to_effort(row)
    }

    pub async fn insert_node(
        &self,
        node: &Node,
        revision: &NodeRevision,
        event: &AuditEvent,
        expected_version: i64,
    ) -> Result<i64, StoreError> {
        let mut tx = self.pool.begin().await?;
        check_version(&mut tx, &node.effort_id, expected_version).await?;
        sqlx::query("INSERT INTO nodes(id,effort_id,kind,title,summary,lifecycle,validity,confidence,confidence_reason,reversibility,impact,uncertainty,cost_of_wrong,current_revision,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&node.id).bind(&node.effort_id).bind(node.kind.as_str()).bind(&node.title)
            .bind(&node.summary).bind(node.lifecycle.as_str()).bind(node.validity.as_str())
            .bind(node.confidence.map(Confidence::as_str)).bind(&node.confidence_reason)
            .bind(node.reversibility.map(Reversibility::as_str)).bind(node.impact.map(RiskLevel::as_str))
            .bind(node.uncertainty.map(Uncertainty::as_str)).bind(node.cost_of_wrong.map(RiskLevel::as_str))
            .bind(node.current_revision).bind(&node.created_at).bind(&node.updated_at)
            .execute(&mut *tx).await?;
        insert_revision(&mut tx, revision).await?;
        insert_event(&mut tx, event).await?;
        let version = bump_version(&mut tx, &node.effort_id, &node.updated_at).await?;
        tx.commit().await?;
        Ok(version)
    }

    pub async fn get_node(&self, effort_id: &str, selector: &str) -> Result<Node, StoreError> {
        let exact = sqlx::query(
            "SELECT n.*,r.body,r.payload_json FROM nodes n JOIN node_revisions r ON r.node_id=n.id AND r.revision=n.current_revision WHERE n.effort_id=? AND n.id=?",
        )
        .bind(effort_id)
        .bind(selector)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(row) = exact {
            return row_to_node(row);
        }
        let rows = sqlx::query(
            "SELECT n.*,r.body,r.payload_json FROM nodes n JOIN node_revisions r ON r.node_id=n.id AND r.revision=n.current_revision WHERE n.effort_id=? AND n.id LIKE ? ORDER BY n.id LIMIT 2",
        )
        .bind(effort_id)
        .bind(format!("{selector}%"))
        .fetch_all(&self.pool)
        .await?;
        if rows.len() != 1 {
            return Err(StoreError::NotFound);
        }
        row_to_node(rows.into_iter().next().expect("one row"))
    }

    pub async fn list_revisions(&self, node_id: &str) -> Result<Vec<NodeRevision>, StoreError> {
        let rows = sqlx::query("SELECT * FROM node_revisions WHERE node_id=? ORDER BY revision")
            .bind(node_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(row_to_revision).collect()
    }

    pub async fn list_revisions_page(
        &self,
        node_id: &str,
        page: Pagination,
    ) -> Result<(Vec<NodeRevision>, Option<u32>), StoreError> {
        let rows = sqlx::query(
            "SELECT * FROM node_revisions WHERE node_id=? ORDER BY revision LIMIT ? OFFSET ?",
        )
        .bind(node_id)
        .bind(page.limit + 1)
        .bind(page.offset)
        .fetch_all(&self.pool)
        .await?;
        let has_more = rows.len() > page.limit as usize;
        let revisions = rows
            .into_iter()
            .take(page.limit as usize)
            .map(row_to_revision)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((
            revisions,
            has_more.then_some(page.offset.saturating_add(page.limit)),
        ))
    }

    pub async fn insert_edge(
        &self,
        edge: &Edge,
        event: &AuditEvent,
        expected_version: i64,
    ) -> Result<i64, StoreError> {
        let mut tx = self.pool.begin().await?;
        check_version(&mut tx, &edge.effort_id, expected_version).await?;
        sqlx::query("INSERT INTO edges(id,effort_id,source_node_id,type,target_node_id,rationale,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)")
            .bind(&edge.id).bind(&edge.effort_id).bind(&edge.source_node_id).bind(edge.edge_type.as_str())
            .bind(&edge.target_node_id).bind(&edge.rationale).bind(&edge.created_by).bind(&edge.created_at)
            .execute(&mut *tx).await?;
        insert_event(&mut tx, event).await?;
        let version = bump_version(&mut tx, &edge.effort_id, &edge.created_at).await?;
        tx.commit().await?;
        Ok(version)
    }

    pub async fn update_node(
        &self,
        node: &mut Node,
        mut revision: Option<&mut NodeRevision>,
        event: &mut AuditEvent,
        expected_version: i64,
        claim_guard: ClaimGuard<'_>,
    ) -> Result<i64, StoreError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let cutoff = reap_expired_claims(&mut tx, &node.effort_id).await?;
        node.updated_at.clone_from(&cutoff);
        if let Some(revision) = revision.as_deref_mut() {
            revision.created_at.clone_from(&cutoff);
        }
        event.occurred_at.clone_from(&cutoff);
        event.after = Some(serde_json::to_value(&*node).expect("node is serializable"));
        check_version(&mut tx, &node.effort_id, expected_version).await?;
        if let ClaimGuard::MustOwn(claimant) | ClaimGuard::OwnIfClaimed(claimant) = claim_guard {
            let owner: Option<String> = sqlx::query_scalar(
                "SELECT claimant FROM claims WHERE node_id=? \
                 AND released_at IS NULL AND julianday(lease_expires_at)>julianday(?)",
            )
            .bind(&node.id)
            .bind(&cutoff)
            .fetch_optional(&mut *tx)
            .await?;
            if owner.as_deref().is_some_and(|owner| owner != claimant)
                || (owner.is_none() && matches!(claim_guard, ClaimGuard::MustOwn(_)))
            {
                return Err(StoreError::ClaimNotOwned(claimant.into()));
            }
        }
        sqlx::query("UPDATE nodes SET title=?,summary=?,lifecycle=?,validity=?,confidence=?,confidence_reason=?,reversibility=?,impact=?,uncertainty=?,cost_of_wrong=?,current_revision=?,updated_at=? WHERE id=?")
            .bind(&node.title).bind(&node.summary).bind(node.lifecycle.as_str()).bind(node.validity.as_str())
            .bind(node.confidence.map(Confidence::as_str)).bind(&node.confidence_reason)
            .bind(node.reversibility.map(Reversibility::as_str)).bind(node.impact.map(RiskLevel::as_str))
            .bind(node.uncertainty.map(Uncertainty::as_str)).bind(node.cost_of_wrong.map(RiskLevel::as_str))
            .bind(node.current_revision).bind(&node.updated_at).bind(&node.id)
            .execute(&mut *tx).await?;
        if let Some(revision) = revision {
            insert_revision(&mut tx, revision).await?;
        }
        if node.lifecycle == Lifecycle::Resolved {
            sqlx::query("UPDATE claims SET released_at=?,release_reason='node resolved' WHERE node_id=? AND released_at IS NULL")
                .bind(&node.updated_at).bind(&node.id).execute(&mut *tx).await?;
        }
        insert_event(&mut tx, event).await?;
        let version = bump_version(&mut tx, &node.effort_id, &node.updated_at).await?;
        tx.commit().await?;
        Ok(version)
    }

    pub async fn insert_claim(&self, claim: &Claim, event: &AuditEvent) -> Result<(), StoreError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let cutoff = reap_expired_claims_for_node(&mut tx, &claim.node_id).await?;
        let active = sqlx::query(
            "SELECT 1 FROM efforts JOIN nodes ON nodes.effort_id=efforts.id \
             WHERE nodes.id=? AND efforts.status='active'",
        )
        .bind(&claim.node_id)
        .fetch_optional(&mut *tx)
        .await?;
        if active.is_none() {
            return Err(StoreError::EffortInactive);
        }
        let result = sqlx::query("INSERT INTO claims(id,node_id,actor_id,claimant,claimed_at,heartbeat_at,lease_expires_at) VALUES(?,?,?,?,?,?,?)")
            .bind(&claim.id).bind(&claim.node_id).bind(&claim.actor_id).bind(&claim.claimant)
            .bind(&claim.claimed_at).bind(&claim.heartbeat_at).bind(&claim.lease_expires_at)
            .execute(&mut *tx).await;
        if let Err(error) = result {
            if error
                .as_database_error()
                .is_some_and(|database| database.is_unique_violation())
            {
                return Err(StoreError::ClaimConflict(claim.node_id.clone()));
            }
            return Err(error.into());
        }
        sqlx::query(
            "UPDATE nodes SET lifecycle='in_progress',updated_at=? WHERE id=? AND lifecycle='open'",
        )
        .bind(&cutoff)
        .bind(&claim.node_id)
        .execute(&mut *tx)
        .await?;
        insert_event(&mut tx, event).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn reap_expired_claims(&self, effort_id: &str) -> Result<(), StoreError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        reap_expired_claims(&mut tx, effort_id).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn release_claim(
        &self,
        claim_id: &str,
        claimant: &str,
        now: &str,
        reason: &str,
        event: &AuditEvent,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row = sqlx::query(
            "SELECT node_id FROM claims WHERE id=? AND claimant=? \
             AND released_at IS NULL AND julianday(lease_expires_at)>julianday('now')",
        )
        .bind(claim_id)
        .bind(claimant)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| StoreError::ClaimNotOwned(claimant.into()))?;
        let node_id: String = row.get("node_id");
        sqlx::query("UPDATE claims SET released_at=?,release_reason=? WHERE id=?")
            .bind(now)
            .bind(reason)
            .bind(claim_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "UPDATE nodes SET lifecycle='open',updated_at=? WHERE id=? AND lifecycle='in_progress'",
        )
        .bind(now)
        .bind(&node_id)
        .execute(&mut *tx)
        .await?;
        insert_event(&mut tx, event).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn heartbeat_claim(
        &self,
        claim_id: &str,
        claimant: &str,
        heartbeat: &str,
        expires: &str,
    ) -> Result<Claim, StoreError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let result = sqlx::query(
            "UPDATE claims SET heartbeat_at=?,lease_expires_at=CASE \
             WHEN julianday(lease_expires_at)>julianday(?) THEN lease_expires_at ELSE ? END \
             WHERE id=? AND claimant=? AND released_at IS NULL AND julianday(lease_expires_at)>julianday('now') \
             AND EXISTS (SELECT 1 FROM nodes JOIN efforts ON efforts.id=nodes.effort_id \
                         WHERE nodes.id=claims.node_id AND efforts.status='active')",
        )
        .bind(heartbeat)
        .bind(expires)
        .bind(expires)
        .bind(claim_id)
        .bind(claimant)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() != 1 {
            return Err(StoreError::ClaimNotOwned(claimant.into()));
        }
        let row = sqlx::query("SELECT * FROM claims WHERE id=?")
            .bind(claim_id)
            .fetch_one(&mut *tx)
            .await?;
        let claim = row_to_claim(row);
        tx.commit().await?;
        Ok(claim)
    }

    pub async fn insert_fog(
        &self,
        fog: &FogPatch,
        event: &AuditEvent,
        expected_version: i64,
    ) -> Result<i64, StoreError> {
        let mut tx = self.pool.begin().await?;
        check_version(&mut tx, &fog.effort_id, expected_version).await?;
        sqlx::query("INSERT INTO fog_patches(id,effort_id,title,description,anchor_node_id,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)")
            .bind(&fog.id).bind(&fog.effort_id).bind(&fog.title).bind(&fog.description)
            .bind(&fog.anchor_node_id).bind(fog.status.as_str()).bind(&fog.created_at).bind(&fog.updated_at)
            .execute(&mut *tx).await?;
        insert_event(&mut tx, event).await?;
        let version = bump_version(&mut tx, &fog.effort_id, &fog.updated_at).await?;
        tx.commit().await?;
        Ok(version)
    }

    pub async fn apply_invalidation(
        &self,
        effort_id: &str,
        updates: &[(Node, NodeRevision)],
        reopened_questions: &[String],
        event: &AuditEvent,
        expected_version: i64,
        now: &str,
    ) -> Result<i64, StoreError> {
        let mut tx = self.pool.begin().await?;
        check_version(&mut tx, effort_id, expected_version).await?;
        for (node, revision) in updates {
            sqlx::query("UPDATE nodes SET lifecycle=CASE WHEN ? THEN 'open' ELSE lifecycle END,validity=?,current_revision=?,updated_at=? WHERE id=? AND effort_id=?")
                .bind(reopened_questions.contains(&node.id))
                .bind(node.validity.as_str())
                .bind(node.current_revision)
                .bind(&node.updated_at)
                .bind(&node.id)
                .bind(effort_id)
                .execute(&mut *tx)
                .await?;
            insert_revision(&mut tx, revision).await?;
        }
        insert_event(&mut tx, event).await?;
        let version = bump_version(&mut tx, effort_id, now).await?;
        tx.commit().await?;
        Ok(version)
    }

    pub async fn insert_source(
        &self,
        source: &Source,
        event: &AuditEvent,
        expected_version: i64,
    ) -> Result<i64, StoreError> {
        let mut tx = self.pool.begin().await?;
        check_version(&mut tx, &source.effort_id, expected_version).await?;
        sqlx::query("INSERT INTO sources(id,effort_id,kind,uri,title,retrieved_at,observed_at,content_hash,excerpt,metadata_json,trust,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&source.id).bind(&source.effort_id).bind(source.kind.as_str()).bind(&source.uri).bind(&source.title)
            .bind(&source.retrieved_at).bind(&source.observed_at).bind(&source.content_hash).bind(&source.excerpt)
            .bind(serde_json::to_string(&source.metadata).expect("JSON value serializes")).bind(source.trust.as_str()).bind(&source.created_at)
            .execute(&mut *tx).await?;
        insert_event(&mut tx, event).await?;
        let version = bump_version(&mut tx, &source.effort_id, &source.created_at).await?;
        tx.commit().await?;
        Ok(version)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn attach_source(
        &self,
        effort_id: &str,
        node_id: &str,
        source_id: &str,
        relationship: &str,
        event: &AuditEvent,
        expected_version: i64,
        now: &str,
    ) -> Result<i64, StoreError> {
        let mut tx = self.pool.begin().await?;
        check_version(&mut tx, effort_id, expected_version).await?;
        sqlx::query("INSERT INTO node_sources(node_id,source_id,relationship) VALUES(?,?,?)")
            .bind(node_id)
            .bind(source_id)
            .bind(relationship)
            .execute(&mut *tx)
            .await?;
        insert_event(&mut tx, event).await?;
        let version = bump_version(&mut tx, effort_id, now).await?;
        tx.commit().await?;
        Ok(version)
    }

    pub async fn insert_criterion(
        &self,
        criterion: &ExitCriterion,
        event: &AuditEvent,
        expected_version: i64,
    ) -> Result<i64, StoreError> {
        let mut tx = self.pool.begin().await?;
        check_version(&mut tx, &criterion.effort_id, expected_version).await?;
        sqlx::query("INSERT INTO exit_criteria(id,effort_id,type,config_json,required,created_at) VALUES(?,?,?,?,?,?)")
            .bind(&criterion.id).bind(&criterion.effort_id).bind(&criterion.criterion_type)
            .bind(serde_json::to_string(&criterion.config).expect("JSON value serializes"))
            .bind(i64::from(criterion.required)).bind(&criterion.created_at).execute(&mut *tx).await?;
        insert_event(&mut tx, event).await?;
        let version = bump_version(&mut tx, &criterion.effort_id, &criterion.created_at).await?;
        tx.commit().await?;
        Ok(version)
    }

    pub async fn insert_finding(
        &self,
        finding: &Finding,
        event: &AuditEvent,
        expected_version: i64,
    ) -> Result<i64, StoreError> {
        let mut tx = self.pool.begin().await?;
        check_version(&mut tx, &finding.effort_id, expected_version).await?;
        sqlx::query("INSERT INTO findings(id,effort_id,type,severity,status,title,detail,related_nodes_json,proposed_by,adjudication,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&finding.id).bind(&finding.effort_id).bind(finding.finding_type.as_str()).bind(finding.severity.as_str())
            .bind(finding.status.as_str()).bind(&finding.title).bind(&finding.detail)
            .bind(serde_json::to_string(&finding.related_nodes).expect("JSON value serializes"))
            .bind(&finding.proposed_by).bind(&finding.adjudication).bind(&finding.created_at).bind(&finding.updated_at)
            .execute(&mut *tx).await?;
        insert_event(&mut tx, event).await?;
        let version = bump_version(&mut tx, &finding.effort_id, &finding.updated_at).await?;
        tx.commit().await?;
        Ok(version)
    }

    pub async fn graduate_fog(
        &self,
        effort_id: &str,
        fog_id: &str,
        node_ids: &[String],
        event: &AuditEvent,
        expected_version: i64,
        now: &str,
    ) -> Result<i64, StoreError> {
        let mut tx = self.pool.begin().await?;
        check_version(&mut tx, effort_id, expected_version).await?;
        sqlx::query("UPDATE fog_patches SET status='graduated',updated_at=? WHERE id=? AND effort_id=? AND status='active'")
            .bind(now).bind(fog_id).bind(effort_id).execute(&mut *tx).await?;
        for node_id in node_ids {
            sqlx::query("INSERT INTO fog_graduations(fog_id,node_id) VALUES(?,?)")
                .bind(fog_id)
                .bind(node_id)
                .execute(&mut *tx)
                .await?;
        }
        insert_event(&mut tx, event).await?;
        let version = bump_version(&mut tx, effort_id, now).await?;
        tx.commit().await?;
        Ok(version)
    }

    pub async fn snapshot(&self, effort_id: &str) -> Result<GraphSnapshot, StoreError> {
        let nodes = sqlx::query("SELECT n.*,r.body,r.payload_json FROM nodes n JOIN node_revisions r ON r.node_id=n.id AND r.revision=n.current_revision WHERE n.effort_id=? ORDER BY n.created_at")
            .bind(effort_id).fetch_all(&self.pool).await?.into_iter().map(row_to_node).collect::<Result<_,_>>()?;
        let edges = sqlx::query("SELECT * FROM edges WHERE effort_id=? ORDER BY created_at")
            .bind(effort_id)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(row_to_edge)
            .collect::<Result<_, _>>()?;
        let claims = sqlx::query("SELECT c.* FROM claims c JOIN nodes n ON n.id=c.node_id WHERE n.effort_id=? ORDER BY c.claimed_at")
            .bind(effort_id).fetch_all(&self.pool).await?.into_iter().map(row_to_claim).collect();
        let fog_patches = self.list_fog(effort_id).await?;
        let findings = sqlx::query("SELECT * FROM findings WHERE effort_id=? ORDER BY created_at")
            .bind(effort_id)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(row_to_finding)
            .collect::<Result<_, _>>()?;
        let exit_criteria =
            sqlx::query("SELECT * FROM exit_criteria WHERE effort_id=? ORDER BY created_at")
                .bind(effort_id)
                .fetch_all(&self.pool)
                .await?
                .into_iter()
                .map(row_to_criterion)
                .collect::<Result<_, _>>()?;
        let source_rows = sqlx::query("SELECT ns.node_id,ns.source_id FROM node_sources ns JOIN nodes n ON n.id=ns.node_id WHERE n.effort_id=?")
            .bind(effort_id).fetch_all(&self.pool).await?;
        let mut node_source_ids = std::collections::HashMap::new();
        for row in source_rows {
            node_source_ids
                .entry(row.get("node_id"))
                .or_insert_with(Vec::new)
                .push(row.get("source_id"));
        }
        Ok(GraphSnapshot {
            nodes,
            edges,
            claims,
            fog_patches,
            findings,
            exit_criteria,
            node_source_ids,
        })
    }

    pub async fn snapshot_bundle(
        &self,
        effort_id: &str,
        include_events: bool,
    ) -> Result<(Effort, GraphSnapshot, Vec<Source>, Vec<AuditEvent>), StoreError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        reap_expired_claims(&mut tx, effort_id).await?;
        let effort = row_to_effort(
            sqlx::query("SELECT * FROM efforts WHERE id=?")
                .bind(effort_id)
                .fetch_one(&mut *tx)
                .await?,
        )?;
        let nodes = sqlx::query("SELECT n.*,r.body,r.payload_json FROM nodes n JOIN node_revisions r ON r.node_id=n.id AND r.revision=n.current_revision WHERE n.effort_id=? ORDER BY n.created_at")
            .bind(effort_id).fetch_all(&mut *tx).await?.into_iter().map(row_to_node).collect::<Result<_,_>>()?;
        let edges = sqlx::query("SELECT * FROM edges WHERE effort_id=? ORDER BY created_at")
            .bind(effort_id)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .map(row_to_edge)
            .collect::<Result<_, _>>()?;
        let claims = sqlx::query("SELECT c.* FROM claims c JOIN nodes n ON n.id=c.node_id WHERE n.effort_id=? ORDER BY c.claimed_at")
            .bind(effort_id).fetch_all(&mut *tx).await?.into_iter().map(row_to_claim).collect();
        let fog_rows =
            sqlx::query("SELECT * FROM fog_patches WHERE effort_id=? ORDER BY created_at")
                .bind(effort_id)
                .fetch_all(&mut *tx)
                .await?;
        let mut fog_patches = Vec::new();
        for row in fog_rows {
            let id: String = row.get("id");
            let graduated_to =
                sqlx::query("SELECT node_id FROM fog_graduations WHERE fog_id=? ORDER BY node_id")
                    .bind(&id)
                    .fetch_all(&mut *tx)
                    .await?
                    .into_iter()
                    .map(|row| row.get("node_id"))
                    .collect();
            fog_patches.push(FogPatch {
                id,
                effort_id: row.get("effort_id"),
                title: row.get("title"),
                description: row.get("description"),
                anchor_node_id: row.get("anchor_node_id"),
                status: parse("fog.status", row.get::<String, _>("status"))?,
                graduated_to,
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            });
        }
        let findings = sqlx::query("SELECT * FROM findings WHERE effort_id=? ORDER BY created_at")
            .bind(effort_id)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .map(row_to_finding)
            .collect::<Result<_, _>>()?;
        let exit_criteria =
            sqlx::query("SELECT * FROM exit_criteria WHERE effort_id=? ORDER BY created_at")
                .bind(effort_id)
                .fetch_all(&mut *tx)
                .await?
                .into_iter()
                .map(row_to_criterion)
                .collect::<Result<_, _>>()?;
        let source_rows = sqlx::query("SELECT ns.node_id,ns.source_id FROM node_sources ns JOIN nodes n ON n.id=ns.node_id WHERE n.effort_id=?")
            .bind(effort_id).fetch_all(&mut *tx).await?;
        let mut node_source_ids = std::collections::HashMap::new();
        for row in source_rows {
            node_source_ids
                .entry(row.get("node_id"))
                .or_insert_with(Vec::new)
                .push(row.get("source_id"));
        }
        let mut events = if include_events {
            sqlx::query("SELECT * FROM events WHERE effort_id=?")
                .bind(effort_id)
                .fetch_all(&mut *tx)
                .await?
                .into_iter()
                .map(row_to_event)
                .collect::<Result<Vec<_>, _>>()?
        } else {
            vec![]
        };
        if include_events {
            sort_events(&mut events)?;
        }
        let sources = sqlx::query("SELECT * FROM sources WHERE effort_id=? ORDER BY created_at")
            .bind(effort_id)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .map(row_to_source)
            .collect::<Result<Vec<_>, _>>()?;
        tx.commit().await?;
        Ok((
            effort,
            GraphSnapshot {
                nodes,
                edges,
                claims,
                fog_patches,
                findings,
                exit_criteria,
                node_source_ids,
            },
            sources,
            events,
        ))
    }

    pub async fn snapshot_section(
        &self,
        effort_id: &str,
        section: &str,
        page: Pagination,
    ) -> Result<(Effort, Vec<Value>, Option<u32>, i64), StoreError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        reap_expired_claims(&mut tx, effort_id).await?;
        let effort = row_to_effort(
            sqlx::query("SELECT * FROM efforts WHERE id=?")
                .bind(effort_id)
                .fetch_one(&mut *tx)
                .await?,
        )?;
        let event_rowid =
            sqlx::query_scalar("SELECT COALESCE(MAX(rowid),0) FROM events WHERE effort_id=?")
                .bind(effort_id)
                .fetch_one(&mut *tx)
                .await?;
        let limit = page.limit.saturating_add(1);
        let mut items = match section {
            "nodes" => sqlx::query("SELECT n.*,r.body,r.payload_json FROM nodes n JOIN node_revisions r ON r.node_id=n.id AND r.revision=n.current_revision WHERE n.effort_id=? ORDER BY n.created_at,n.id LIMIT ? OFFSET ?")
                .bind(effort_id).bind(limit).bind(page.offset).fetch_all(&mut *tx).await?
                .into_iter().map(row_to_node).map(|value| value.map(|value| serde_json::to_value(value).expect("node serializes"))).collect::<Result<_,_>>()?,
            "edges" => sqlx::query("SELECT * FROM edges WHERE effort_id=? ORDER BY created_at,id LIMIT ? OFFSET ?")
                .bind(effort_id).bind(limit).bind(page.offset).fetch_all(&mut *tx).await?
                .into_iter().map(row_to_edge).map(|value| value.map(|value| serde_json::to_value(value).expect("edge serializes"))).collect::<Result<_,_>>()?,
            "claims" => sqlx::query("SELECT c.* FROM claims c JOIN nodes n ON n.id=c.node_id WHERE n.effort_id=? ORDER BY c.claimed_at,c.id LIMIT ? OFFSET ?")
                .bind(effort_id).bind(limit).bind(page.offset).fetch_all(&mut *tx).await?
                .into_iter().map(row_to_claim).map(|value| serde_json::to_value(value).expect("claim serializes")).collect(),
            "fog_patches" => {
                let rows = sqlx::query("SELECT * FROM fog_patches WHERE effort_id=? ORDER BY created_at,id LIMIT ? OFFSET ?")
                    .bind(effort_id).bind(limit).bind(page.offset).fetch_all(&mut *tx).await?;
                let mut items = Vec::with_capacity(rows.len());
                for row in rows {
                    let id: String = row.get("id");
                    let graduated_to = sqlx::query("SELECT node_id FROM fog_graduations WHERE fog_id=? ORDER BY node_id")
                        .bind(&id).fetch_all(&mut *tx).await?.into_iter().map(|row| row.get("node_id")).collect();
                    items.push(serde_json::to_value(FogPatch {
                        id,
                        effort_id: row.get("effort_id"),
                        title: row.get("title"),
                        description: row.get("description"),
                        anchor_node_id: row.get("anchor_node_id"),
                        status: parse("fog.status", row.get::<String, _>("status"))?,
                        graduated_to,
                        created_at: row.get("created_at"),
                        updated_at: row.get("updated_at"),
                    }).expect("fog patch serializes"));
                }
                items
            }
            "findings" => sqlx::query("SELECT * FROM findings WHERE effort_id=? ORDER BY created_at,id LIMIT ? OFFSET ?")
                .bind(effort_id).bind(limit).bind(page.offset).fetch_all(&mut *tx).await?
                .into_iter().map(row_to_finding).map(|value| value.map(|value| serde_json::to_value(value).expect("finding serializes"))).collect::<Result<_,_>>()?,
            "exit_criteria" => sqlx::query("SELECT * FROM exit_criteria WHERE effort_id=? ORDER BY created_at,id LIMIT ? OFFSET ?")
                .bind(effort_id).bind(limit).bind(page.offset).fetch_all(&mut *tx).await?
                .into_iter().map(row_to_criterion).map(|value| value.map(|value| serde_json::to_value(value).expect("criterion serializes"))).collect::<Result<_,_>>()?,
            "sources" => sqlx::query("SELECT * FROM sources WHERE effort_id=? ORDER BY created_at,id LIMIT ? OFFSET ?")
                .bind(effort_id).bind(limit).bind(page.offset).fetch_all(&mut *tx).await?
                .into_iter().map(row_to_source).map(|value| value.map(|value| serde_json::to_value(value).expect("source serializes"))).collect::<Result<_,_>>()?,
            "node_sources" => sqlx::query("SELECT ns.node_id,ns.source_id FROM node_sources ns JOIN nodes n ON n.id=ns.node_id WHERE n.effort_id=? ORDER BY ns.node_id,ns.source_id LIMIT ? OFFSET ?")
                .bind(effort_id).bind(limit).bind(page.offset).fetch_all(&mut *tx).await?
                .into_iter().map(|row| json!({"node_id":row.get::<String,_>("node_id"),"source_id":row.get::<String,_>("source_id")})).collect(),
            _ => return Err(StoreError::NotFound),
        };
        let has_more = items.len() > page.limit as usize;
        items.truncate(page.limit as usize);
        let next = has_more.then_some(page.offset.saturating_add(page.limit));
        tx.commit().await?;
        Ok((effort, items, next, event_rowid))
    }

    pub async fn list_fog(&self, effort_id: &str) -> Result<Vec<FogPatch>, StoreError> {
        let rows = sqlx::query("SELECT * FROM fog_patches WHERE effort_id=? ORDER BY created_at")
            .bind(effort_id)
            .fetch_all(&self.pool)
            .await?;
        let mut patches = Vec::new();
        for row in rows {
            let id: String = row.get("id");
            let graduated_to =
                sqlx::query("SELECT node_id FROM fog_graduations WHERE fog_id=? ORDER BY node_id")
                    .bind(&id)
                    .fetch_all(&self.pool)
                    .await?
                    .into_iter()
                    .map(|row| row.get("node_id"))
                    .collect();
            patches.push(FogPatch {
                id,
                effort_id: row.get("effort_id"),
                title: row.get("title"),
                description: row.get("description"),
                anchor_node_id: row.get("anchor_node_id"),
                status: parse("fog.status", row.get::<String, _>("status"))?,
                graduated_to,
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            });
        }
        Ok(patches)
    }

    pub async fn list_events(
        &self,
        workspace_id: &str,
        filter: &EventFilter,
    ) -> Result<Vec<AuditEvent>, StoreError> {
        Ok(self.query_events(workspace_id, filter, None).await?.0)
    }

    pub async fn list_events_page(
        &self,
        workspace_id: &str,
        filter: &EventFilter,
        page: EventPage,
    ) -> Result<(Vec<AuditEvent>, Option<EventCursor>), StoreError> {
        self.query_events(workspace_id, filter, Some(page)).await
    }

    async fn query_events(
        &self,
        workspace_id: &str,
        filter: &EventFilter,
        page: Option<EventPage>,
    ) -> Result<(Vec<AuditEvent>, Option<EventCursor>), StoreError> {
        let occurred_from = filter
            .occurred_from
            .as_deref()
            .map(parse_timestamp)
            .transpose()?;
        let occurred_to = filter
            .occurred_to
            .as_deref()
            .map(parse_timestamp)
            .transpose()?;
        let occurred_from_candidate = occurred_from.and_then(|value| time_candidate(value, -1));
        let occurred_to_candidate = occurred_to.and_then(|value| time_candidate(value, 1));
        if page
            .as_ref()
            .and_then(|page| page.cursor.as_ref())
            .is_some_and(|cursor| cursor.filter != *filter)
        {
            return Err(StoreError::CursorFilterMismatch);
        }
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT events.* FROM events JOIN efforts ON efforts.id=events.effort_id \
             WHERE efforts.workspace_id=",
        );
        query.push_bind(workspace_id);
        let through_rowid = match &page {
            Some(page) => match &page.cursor {
                Some(cursor) => cursor.through_rowid,
                None => {
                    sqlx::query_scalar("SELECT COALESCE(MAX(rowid),0) FROM events")
                        .fetch_one(&self.pool)
                        .await?
                }
            },
            None => 0,
        };
        if page.is_some() {
            query.push(" AND events.rowid<=").push_bind(through_rowid);
        }
        if let Some(value) = &filter.effort_id {
            query.push(" AND events.effort_id=").push_bind(value);
        }
        if let Some(value) = &filter.entity_type {
            query.push(" AND events.entity_type=").push_bind(value);
        }
        if let Some(value) = &filter.entity_id {
            query.push(" AND events.entity_id=").push_bind(value);
        }
        if let Some(value) = &filter.actor_id {
            query.push(" AND events.actor_id=").push_bind(value);
        }
        if let Some(value) = &filter.event_type {
            query.push(" AND events.event_type=").push_bind(value);
        }
        if let Some(value) = &occurred_from_candidate {
            query.push(" AND events.occurred_at>").push_bind(value);
        }
        if let Some(value) = &occurred_to_candidate {
            query.push(" AND events.occurred_at<").push_bind(value);
        }
        if let Some(cursor) = page.as_ref().and_then(|page| page.cursor.as_ref()) {
            query
                .push(" AND (events.occurred_at>")
                .push_bind(&cursor.occurred_at)
                .push(" OR (events.occurred_at=")
                .push_bind(&cursor.occurred_at)
                .push(" AND events.id>")
                .push_bind(&cursor.id)
                .push("))");
        }
        query.push(" ORDER BY events.occurred_at,events.id");
        if let Some(page) = &page {
            query
                .push(" LIMIT ")
                .push_bind(page.limit.saturating_add(1));
        }
        let rows = query.build().fetch_all(&self.pool).await?;
        let has_more = page
            .as_ref()
            .is_some_and(|page| rows.len() > page.limit as usize);
        let row_limit = page.as_ref().map_or(rows.len(), |page| page.limit as usize);
        let mut events = Vec::with_capacity(rows.len());
        let mut last_seen = None;
        for row in rows.into_iter().take(row_limit) {
            let event = row_to_event(row)?;
            last_seen = Some((event.occurred_at.clone(), event.id.clone()));
            let occurred_at = parse_timestamp(&event.occurred_at)?;
            if occurred_from.is_some_and(|bound| occurred_at < bound)
                || occurred_to.is_some_and(|bound| occurred_at > bound)
            {
                continue;
            }
            events.push((occurred_at, event));
        }
        let mut events = events.into_iter().map(|(_, event)| event).collect();
        sort_events(&mut events)?;
        let next_cursor = has_more.then(|| {
            let (occurred_at, id) = last_seen.expect("a full page has a last event");
            EventCursor {
                occurred_at,
                id,
                through_rowid,
                filter: filter.clone(),
            }
        });
        Ok((events, next_cursor))
    }

    pub async fn list_sources(&self, effort_id: &str) -> Result<Vec<Source>, StoreError> {
        let rows = sqlx::query("SELECT * FROM sources WHERE effort_id=? ORDER BY created_at")
            .bind(effort_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(row_to_source).collect()
    }
}

async fn reap_expired_claims(
    tx: &mut Transaction<'_, Sqlite>,
    effort_id: &str,
) -> Result<String, StoreError> {
    let timestamp: String = sqlx::query_scalar("SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now')")
        .fetch_one(&mut **tx)
        .await?;
    let claims = sqlx::query(
        "SELECT claims.id,claims.node_id FROM claims \
         JOIN nodes ON nodes.id=claims.node_id \
         WHERE nodes.effort_id=? AND claims.released_at IS NULL \
         AND julianday(claims.lease_expires_at)<=julianday(?)",
    )
    .bind(effort_id)
    .bind(&timestamp)
    .fetch_all(&mut **tx)
    .await?;
    for claim in claims {
        let claim_id: String = claim.get("id");
        let node_id: String = claim.get("node_id");
        sqlx::query("UPDATE claims SET released_at=?,release_reason='lease expired' WHERE id=?")
            .bind(&timestamp)
            .bind(&claim_id)
            .execute(&mut **tx)
            .await?;
        sqlx::query(
            "UPDATE nodes SET lifecycle='open',updated_at=? \
             WHERE id=? AND lifecycle='in_progress'",
        )
        .bind(&timestamp)
        .bind(&node_id)
        .execute(&mut **tx)
        .await?;
        insert_event(
            tx,
            &AuditEvent {
                id: Ulid::new().to_string(),
                effort_id: Some(effort_id.into()),
                actor_id: "system".into(),
                session_id: None,
                event_type: "claim_expired".into(),
                entity_type: "claim".into(),
                entity_id: claim_id,
                before: None,
                after: None,
                reason: Some("lease expired".into()),
                occurred_at: timestamp.clone(),
            },
        )
        .await?;
    }
    Ok(timestamp)
}

async fn reap_expired_claims_for_node(
    tx: &mut Transaction<'_, Sqlite>,
    node_id: &str,
) -> Result<String, StoreError> {
    let effort_id: String = sqlx::query_scalar("SELECT effort_id FROM nodes WHERE id=?")
        .bind(node_id)
        .fetch_one(&mut **tx)
        .await?;
    reap_expired_claims(tx, &effort_id).await
}

async fn check_version(
    tx: &mut Transaction<'_, Sqlite>,
    effort_id: &str,
    expected: i64,
) -> Result<(), StoreError> {
    let row = sqlx::query("SELECT version,status FROM efforts WHERE id=?")
        .bind(effort_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(StoreError::NotFound)?;
    let actual: i64 = row.get("version");
    if actual != expected {
        return Err(StoreError::VersionConflict { expected, actual });
    }
    let status: String = row.get("status");
    if status != "active" {
        return Err(StoreError::EffortInactive);
    }
    Ok(())
}

async fn bump_version(
    tx: &mut Transaction<'_, Sqlite>,
    effort_id: &str,
    now: &str,
) -> Result<i64, StoreError> {
    sqlx::query("UPDATE efforts SET version=version+1,updated_at=? WHERE id=?")
        .bind(now)
        .bind(effort_id)
        .execute(&mut **tx)
        .await?;
    let row = sqlx::query("SELECT version FROM efforts WHERE id=?")
        .bind(effort_id)
        .fetch_one(&mut **tx)
        .await?;
    Ok(row.get("version"))
}

async fn insert_revision(
    tx: &mut Transaction<'_, Sqlite>,
    revision: &NodeRevision,
) -> Result<(), StoreError> {
    sqlx::query("INSERT INTO node_revisions(node_id,revision,body,payload_json,reason,actor_id,session_id,created_at) VALUES(?,?,?,?,?,?,?,?)")
        .bind(&revision.node_id).bind(revision.revision).bind(&revision.body)
        .bind(serde_json::to_string(&revision.payload).expect("JSON value serializes"))
        .bind(&revision.reason).bind(&revision.actor_id).bind(&revision.session_id).bind(&revision.created_at)
        .execute(&mut **tx).await?;
    Ok(())
}

async fn insert_event(
    tx: &mut Transaction<'_, Sqlite>,
    event: &AuditEvent,
) -> Result<(), StoreError> {
    let occurred_at = normalize_timestamp(&event.occurred_at)?;
    sqlx::query("INSERT INTO events(id,effort_id,actor_id,session_id,event_type,entity_type,entity_id,before_json,after_json,reason,occurred_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)")
        .bind(&event.id).bind(&event.effort_id).bind(&event.actor_id).bind(&event.session_id)
        .bind(&event.event_type).bind(&event.entity_type).bind(&event.entity_id)
        .bind(event.before.as_ref().map(|value| serde_json::to_string(value).expect("JSON value serializes")))
        .bind(event.after.as_ref().map(|value| serde_json::to_string(value).expect("JSON value serializes")))
        .bind(&event.reason).bind(occurred_at).execute(&mut **tx).await?;
    Ok(())
}

fn row_to_workspace(row: SqliteRow) -> Workspace {
    Workspace {
        id: row.get("id"),
        name: row.get("name"),
        root_uri: row.get("root_uri"),
        schema_version: row.get("schema_version"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn row_to_effort(row: SqliteRow) -> Result<Effort, StoreError> {
    Ok(Effort {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        slug: row.get("slug"),
        title: row.get("title"),
        destination: row.get("destination"),
        scope_notes: row.get("scope_notes"),
        status: parse("effort.status", row.get::<String, _>("status"))?,
        version: row.get("version"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn row_to_node(row: SqliteRow) -> Result<Node, StoreError> {
    Ok(Node {
        id: row.get("id"),
        effort_id: row.get("effort_id"),
        kind: parse("node.kind", row.get::<String, _>("kind"))?,
        title: row.get("title"),
        summary: row.get("summary"),
        lifecycle: parse("node.lifecycle", row.get::<String, _>("lifecycle"))?,
        validity: parse("node.validity", row.get::<String, _>("validity"))?,
        confidence: parse_optional("node.confidence", row.get("confidence"))?,
        confidence_reason: row.get("confidence_reason"),
        reversibility: parse_optional("node.reversibility", row.get("reversibility"))?,
        impact: parse_optional("node.impact", row.get("impact"))?,
        uncertainty: parse_optional("node.uncertainty", row.get("uncertainty"))?,
        cost_of_wrong: parse_optional("node.cost_of_wrong", row.get("cost_of_wrong"))?,
        current_revision: row.get("current_revision"),
        body: row.get("body"),
        payload: parse_json("node.payload", row.get("payload_json"))?,
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn row_to_revision(row: SqliteRow) -> Result<NodeRevision, StoreError> {
    Ok(NodeRevision {
        node_id: row.get("node_id"),
        revision: row.get("revision"),
        body: row.get("body"),
        payload: parse_json("revision.payload", row.get("payload_json"))?,
        reason: row.get("reason"),
        actor_id: row.get("actor_id"),
        session_id: row.get("session_id"),
        created_at: row.get("created_at"),
    })
}

fn row_to_edge(row: SqliteRow) -> Result<Edge, StoreError> {
    Ok(Edge {
        id: row.get("id"),
        effort_id: row.get("effort_id"),
        source_node_id: row.get("source_node_id"),
        edge_type: parse("edge.type", row.get::<String, _>("type"))?,
        target_node_id: row.get("target_node_id"),
        rationale: row.get("rationale"),
        created_by: row.get("created_by"),
        created_at: row.get("created_at"),
    })
}

fn row_to_claim(row: SqliteRow) -> Claim {
    Claim {
        id: row.get("id"),
        node_id: row.get("node_id"),
        actor_id: row.get("actor_id"),
        claimant: row.get("claimant"),
        claimed_at: row.get("claimed_at"),
        heartbeat_at: row.get("heartbeat_at"),
        lease_expires_at: row.get("lease_expires_at"),
        released_at: row.get("released_at"),
        release_reason: row.get("release_reason"),
    }
}

fn row_to_finding(row: SqliteRow) -> Result<Finding, StoreError> {
    Ok(Finding {
        id: row.get("id"),
        effort_id: row.get("effort_id"),
        finding_type: parse("finding.type", row.get::<String, _>("type"))?,
        severity: parse("finding.severity", row.get::<String, _>("severity"))?,
        status: parse("finding.status", row.get::<String, _>("status"))?,
        title: row.get("title"),
        detail: row.get("detail"),
        related_nodes: parse_json("finding.related_nodes", row.get("related_nodes_json"))?,
        proposed_by: row.get("proposed_by"),
        adjudication: row.get("adjudication"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn row_to_criterion(row: SqliteRow) -> Result<ExitCriterion, StoreError> {
    Ok(ExitCriterion {
        id: row.get("id"),
        effort_id: row.get("effort_id"),
        criterion_type: row.get("type"),
        config: parse_json("criterion.config", row.get("config_json"))?,
        required: row.get::<i64, _>("required") != 0,
        created_at: row.get("created_at"),
    })
}

fn row_to_event(row: SqliteRow) -> Result<AuditEvent, StoreError> {
    Ok(AuditEvent {
        id: row.get("id"),
        effort_id: row.get("effort_id"),
        actor_id: row.get("actor_id"),
        session_id: row.get("session_id"),
        event_type: row.get("event_type"),
        entity_type: row.get("entity_type"),
        entity_id: row.get("entity_id"),
        before: parse_optional_json("event.before", row.get("before_json"))?,
        after: parse_optional_json("event.after", row.get("after_json"))?,
        reason: row.get("reason"),
        occurred_at: row.get("occurred_at"),
    })
}

fn row_to_source(row: SqliteRow) -> Result<Source, StoreError> {
    Ok(Source {
        id: row.get("id"),
        effort_id: row.get("effort_id"),
        kind: parse("source.kind", row.get::<String, _>("kind"))?,
        uri: row.get("uri"),
        title: row.get("title"),
        retrieved_at: row.get("retrieved_at"),
        observed_at: row.get("observed_at"),
        content_hash: row.get("content_hash"),
        excerpt: row.get("excerpt"),
        metadata: parse_json("source.metadata", row.get("metadata_json"))?,
        trust: parse("source.trust", row.get::<String, _>("trust"))?,
        created_at: row.get("created_at"),
    })
}

fn parse<T: FromStr<Err = String>>(field: &'static str, value: String) -> Result<T, StoreError> {
    value
        .parse()
        .map_err(|_| StoreError::InvalidEnum { field, value })
}

fn parse_optional<T: FromStr<Err = String>>(
    field: &'static str,
    value: Option<String>,
) -> Result<Option<T>, StoreError> {
    value.map(|value| parse(field, value)).transpose()
}

fn parse_json<T: serde::de::DeserializeOwned>(
    field: &'static str,
    value: String,
) -> Result<T, StoreError> {
    serde_json::from_str(&value).map_err(|source| StoreError::InvalidJson { field, source })
}

fn parse_optional_json(
    field: &'static str,
    value: Option<String>,
) -> Result<Option<Value>, StoreError> {
    value.map(|value| parse_json(field, value)).transpose()
}

fn parse_timestamp(value: &str) -> Result<OffsetDateTime, StoreError> {
    OffsetDateTime::parse(value, &Rfc3339).map_err(|_| StoreError::InvalidTimestamp(value.into()))
}

fn normalize_timestamp(value: &str) -> Result<String, StoreError> {
    parse_timestamp(value)?
        .to_offset(UtcOffset::UTC)
        .format(time::macros::format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:9]Z"
        ))
        .map_err(|_| StoreError::InvalidTimestamp(value.into()))
}

async fn normalize_event_timestamps(pool: &SqlitePool) -> Result<(), StoreError> {
    let completed: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM store_metadata WHERE key='event_timestamps_normalized')",
    )
    .fetch_one(pool)
    .await?;
    if completed {
        return Ok(());
    }
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    let completed: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM store_metadata WHERE key='event_timestamps_normalized')",
    )
    .fetch_one(&mut *tx)
    .await?;
    if completed {
        tx.commit().await?;
        return Ok(());
    }
    let rows = sqlx::query("SELECT id,occurred_at FROM events")
        .fetch_all(&mut *tx)
        .await?;
    for row in rows {
        let id: String = row.get("id");
        let occurred_at: String = row.get("occurred_at");
        let normalized = normalize_timestamp(&occurred_at)?;
        if normalized != occurred_at {
            sqlx::query("UPDATE events SET occurred_at=? WHERE id=?")
                .bind(normalized)
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
    }
    sqlx::query("INSERT INTO store_metadata(key,value) VALUES('event_timestamps_normalized','1')")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

fn time_candidate(value: OffsetDateTime, seconds: i64) -> Option<String> {
    let value = value
        .to_offset(UtcOffset::UTC)
        .checked_add(TimeDuration::seconds(seconds))?;
    Some(
        OffsetDateTime::from_unix_timestamp(value.unix_timestamp())
            .expect("parsed timestamp is within the Unix timestamp range")
            .format(&Rfc3339)
            .expect("RFC 3339 formatting succeeds"),
    )
}

fn sort_events(events: &mut Vec<AuditEvent>) -> Result<(), StoreError> {
    let mut parsed = events
        .drain(..)
        .map(|event| Ok((parse_timestamp(&event.occurred_at)?, event)))
        .collect::<Result<Vec<_>, StoreError>>()?;
    parsed.sort_by(|(left_time, left), (right_time, right)| {
        left_time
            .cmp(right_time)
            .then_with(|| left.id.cmp(&right.id))
    });
    events.extend(parsed.into_iter().map(|(_, event)| event));
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::TempDir;
    use threadmark_domain::{AuditEvent, Lifecycle, NodeRevision, Validity, Workspace};

    use super::*;

    #[tokio::test]
    async fn reconciles_concurrent_first_opens() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("state.sqlite3");
        let first = Store::connect(&path).await.unwrap();
        let second = Store::connect(&path).await.unwrap();
        let workspace = |root_uri: &str| Workspace {
            id: "01TESTWORKSPACE000000000000".into(),
            name: "test".into(),
            root_uri: root_uri.into(),
            schema_version: 1,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        };

        let first_workspace = workspace("first-worktree");
        let second_workspace = workspace("second-worktree");
        let (first_result, second_result) = tokio::join!(
            first.reconcile_workspace(&first_workspace),
            second.reconcile_workspace(&second_workspace),
        );

        first_result.unwrap();
        second_result.unwrap();
        assert_eq!(
            first
                .get_workspace("01TESTWORKSPACE000000000000")
                .await
                .unwrap()
                .id,
            "01TESTWORKSPACE000000000000"
        );
    }

    #[tokio::test]
    async fn invalidation_does_not_restore_a_released_claim_lifecycle() {
        let directory = TempDir::new().unwrap();
        let store = Store::connect(&directory.path().join("state.sqlite3"))
            .await
            .unwrap();
        sqlx::query("INSERT INTO workspaces(id,name,root_uri,schema_version,created_at,updated_at) VALUES('workspace','test','test',1,'now','now')")
            .execute(&store.pool).await.unwrap();
        sqlx::query("INSERT INTO efforts(id,workspace_id,slug,title,destination,status,version,created_at,updated_at) VALUES('effort','workspace','test','test','test','active',1,'now','now')")
            .execute(&store.pool).await.unwrap();
        sqlx::query("INSERT INTO nodes(id,effort_id,kind,title,lifecycle,validity,current_revision,created_at,updated_at) VALUES('node','effort','action','test','in_progress','current',0,'now','now')")
            .execute(&store.pool).await.unwrap();
        sqlx::query("INSERT INTO node_revisions(node_id,revision,body,payload_json,actor_id,created_at) VALUES('node',0,'','{}','test','now')")
            .execute(&store.pool).await.unwrap();

        let mut stale = store.get_node("effort", "node").await.unwrap();
        stale.validity = Validity::Invalid;
        stale.current_revision = 1;
        sqlx::query("UPDATE nodes SET lifecycle='open' WHERE id='node'")
            .execute(&store.pool)
            .await
            .unwrap();
        let revision = NodeRevision {
            node_id: stale.id.clone(),
            revision: stale.current_revision,
            body: String::new(),
            payload: json!({}),
            reason: Some("invalidated".into()),
            actor_id: "test".into(),
            session_id: None,
            created_at: "later".into(),
        };
        let event = AuditEvent {
            id: "event".into(),
            effort_id: Some("effort".into()),
            actor_id: "test".into(),
            session_id: None,
            event_type: "invalidation_committed".into(),
            entity_type: "node".into(),
            entity_id: stale.id.clone(),
            before: None,
            after: None,
            reason: None,
            occurred_at: "2026-01-01T00:00:00Z".into(),
        };

        store
            .apply_invalidation("effort", &[(stale, revision)], &[], &event, 1, "later")
            .await
            .unwrap();
        assert_eq!(
            store.get_node("effort", "node").await.unwrap().lifecycle,
            Lifecycle::Open
        );
    }

    #[tokio::test]
    async fn reaping_an_expired_claim_reopens_its_node() {
        let directory = TempDir::new().unwrap();
        let store = Store::connect(&directory.path().join("state.sqlite3"))
            .await
            .unwrap();
        sqlx::query("INSERT INTO workspaces(id,name,root_uri,schema_version,created_at,updated_at) VALUES('workspace','test','test',1,'now','now')")
            .execute(&store.pool).await.unwrap();
        sqlx::query("INSERT INTO efforts(id,workspace_id,slug,title,destination,status,version,created_at,updated_at) VALUES('effort','workspace','test','test','test','active',1,'now','now')")
            .execute(&store.pool).await.unwrap();
        sqlx::query("INSERT INTO nodes(id,effort_id,kind,title,lifecycle,validity,current_revision,created_at,updated_at) VALUES('node','effort','action','test','in_progress','current',0,'now','now')")
            .execute(&store.pool).await.unwrap();
        sqlx::query("INSERT INTO node_revisions(node_id,revision,body,payload_json,actor_id,created_at) VALUES('node',0,'','{}','test','now')")
            .execute(&store.pool).await.unwrap();
        sqlx::query("INSERT INTO claims(id,node_id,actor_id,claimant,claimed_at,heartbeat_at,lease_expires_at) VALUES('claim','node','openai-codex','openai-codex','then','then','2020')")
            .execute(&store.pool).await.unwrap();

        store.reap_expired_claims("effort").await.unwrap();

        assert_eq!(
            store.get_node("effort", "node").await.unwrap().lifecycle,
            Lifecycle::Open
        );
        let released_at: Option<String> =
            sqlx::query_scalar("SELECT released_at FROM claims WHERE id='claim'")
                .fetch_one(&store.pool)
                .await
                .unwrap();
        assert!(released_at.is_some());
        let expired: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events WHERE event_type='claim_expired' AND entity_id='claim'",
        )
        .fetch_one(&store.pool)
        .await
        .unwrap();
        assert_eq!(expired, 1);
    }

    #[tokio::test]
    async fn filters_event_times_as_instants_without_losing_precision() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("state.sqlite3");
        let store = Store::connect(&path).await.unwrap();
        sqlx::query("INSERT INTO workspaces(id,name,root_uri,schema_version,created_at,updated_at) VALUES('workspace','test','test',1,'now','now')")
            .execute(&store.pool).await.unwrap();
        sqlx::query("INSERT INTO efforts(id,workspace_id,slug,title,destination,status,version,created_at,updated_at) VALUES('effort','workspace','test','test','test','active',1,'now','now')")
            .execute(&store.pool).await.unwrap();
        sqlx::query("INSERT INTO events(id,effort_id,actor_id,event_type,entity_type,entity_id,occurred_at) VALUES('event','effort','test','node_created','node','node','2026-01-01T00:30:00.1231Z')")
            .execute(&store.pool).await.unwrap();
        sqlx::query("INSERT INTO events(id,effort_id,actor_id,event_type,entity_type,entity_id,occurred_at) VALUES('early','effort','test','ordered','node','node','2026-01-01T00:30:00.123Z'),('late','effort','test','ordered','node','node','2026-01-01T00:30:00.123456Z')")
            .execute(&store.pool).await.unwrap();
        sqlx::query("DELETE FROM store_metadata WHERE key='event_timestamps_normalized'")
            .execute(&store.pool)
            .await
            .unwrap();
        drop(store);
        let store = Store::connect(&path).await.unwrap();

        let events = store
            .list_events(
                "workspace",
                &EventFilter {
                    event_type: Some("node_created".into()),
                    occurred_from: Some("2026-01-01T01:00:00+01:00".into()),
                    ..EventFilter::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        let events = store
            .list_events(
                "workspace",
                &EventFilter {
                    event_type: Some("node_created".into()),
                    occurred_from: Some("2026-01-01T00:30:00.1234Z".into()),
                    ..EventFilter::default()
                },
            )
            .await
            .unwrap();
        assert!(events.is_empty());

        let events = store
            .list_events(
                "workspace",
                &EventFilter {
                    event_type: Some("ordered".into()),
                    ..EventFilter::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            events.iter().map(|event| &event.id).collect::<Vec<_>>(),
            ["early", "late"]
        );
        let filter = EventFilter {
            event_type: Some("ordered".into()),
            ..EventFilter::default()
        };

        let (first, next) = store
            .list_events_page(
                "workspace",
                &filter,
                EventPage {
                    limit: 1,
                    cursor: None,
                },
            )
            .await
            .unwrap();
        sqlx::query("INSERT INTO events(id,effort_id,actor_id,event_type,entity_type,entity_id,occurred_at) VALUES('inserted','effort','test','ordered','node','node','2026-01-01T00:30:00.122000000Z')")
            .execute(&store.pool).await.unwrap();
        let (second, _) = store
            .list_events_page(
                "workspace",
                &filter,
                EventPage {
                    limit: 1,
                    cursor: next,
                },
            )
            .await
            .unwrap();

        assert_eq!(first[0].id, "early");
        assert_eq!(second[0].id, "late");
    }

    #[tokio::test]
    async fn reaping_does_not_reopen_an_unclaimed_node() {
        let directory = TempDir::new().unwrap();
        let store = Store::connect(&directory.path().join("state.sqlite3"))
            .await
            .unwrap();
        sqlx::query("INSERT INTO workspaces(id,name,root_uri,schema_version,created_at,updated_at) VALUES('workspace','test','test',1,'now','now')")
            .execute(&store.pool).await.unwrap();
        sqlx::query("INSERT INTO efforts(id,workspace_id,slug,title,destination,status,version,created_at,updated_at) VALUES('effort','workspace','test','test','test','active',1,'now','now')")
            .execute(&store.pool).await.unwrap();
        sqlx::query("INSERT INTO nodes(id,effort_id,kind,title,lifecycle,validity,current_revision,created_at,updated_at) VALUES('node','effort','action','test','in_progress','current',0,'then','then')")
            .execute(&store.pool).await.unwrap();

        store.reap_expired_claims("effort").await.unwrap();

        let row = sqlx::query("SELECT lifecycle,updated_at FROM nodes WHERE id='node'")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(row.get::<String, _>("lifecycle"), "in_progress");
        assert_eq!(row.get::<String, _>("updated_at"), "then");
    }

    #[tokio::test]
    async fn claimant_migration_retires_legacy_claims_and_reopens_their_nodes() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::raw_sql(
            "CREATE TABLE nodes (id TEXT PRIMARY KEY, lifecycle TEXT NOT NULL, updated_at TEXT NOT NULL); \
             CREATE TABLE claims (id TEXT PRIMARY KEY, node_id TEXT NOT NULL, session_id TEXT NOT NULL, lease_expires_at TEXT NOT NULL, released_at TEXT, release_reason TEXT); \
             INSERT INTO nodes VALUES ('claimed','in_progress','then'),('unclaimed','in_progress','then'),('released','in_progress','then'); \
             INSERT INTO claims VALUES ('active','claimed','session','future',NULL,NULL),('old','released','session','past','past','done');",
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::raw_sql(include_str!("../migrations/0002_claimant.sql"))
            .execute(&pool)
            .await
            .unwrap();

        let nodes: Vec<(String, String)> =
            sqlx::query_as("SELECT id,lifecycle FROM nodes ORDER BY id")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            nodes,
            vec![
                ("claimed".into(), "open".into()),
                ("released".into(), "in_progress".into()),
                ("unclaimed".into(), "in_progress".into()),
            ]
        );
        let active: (Option<String>, Option<String>) =
            sqlx::query_as("SELECT released_at,release_reason FROM claims WHERE id='active'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(active.0.is_some());
        assert_eq!(active.1.as_deref(), Some("claimant migration"));
        let claimant: String = sqlx::query_scalar("SELECT claimant FROM claims WHERE id='active'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(claimant, "session");
    }
}

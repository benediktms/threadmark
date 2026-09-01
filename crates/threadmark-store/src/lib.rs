//! SQLite persistence for Threadmark.

use std::{path::Path, str::FromStr, time::Duration};

use serde_json::Value;
use sqlx::{
    Row, Sqlite, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow},
};
use thiserror::Error;
use threadmark_domain::{
    AuditEvent, Claim, Confidence, Edge, Effort, ExitCriterion, Finding, FogPatch, GraphSnapshot,
    InvalidationPreview, Lifecycle, Node, NodeRevision, Reversibility, RiskLevel, Source,
    Uncertainty, Workspace,
};

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
    #[error("active claim already exists for node {0}")]
    ClaimConflict(String),
    #[error("entity was not found")]
    NotFound,
    #[error("effort version conflict: expected {expected}, actual {actual}")]
    VersionConflict { expected: i64, actual: i64 },
}

#[derive(Clone, Debug)]
pub struct Store {
    pool: SqlitePool,
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
        let mut tx = self.pool.begin().await?;
        let existing = sqlx::query("SELECT * FROM workspaces WHERE id = ?")
            .bind(&workspace.id)
            .fetch_optional(&mut *tx)
            .await?;

        if let Some(row) = existing {
            let existing = row_to_workspace(row);
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
        } else if let Some(row) = sqlx::query("SELECT * FROM workspaces WHERE root_uri = ?")
            .bind(&workspace.root_uri)
            .fetch_optional(&mut *tx)
            .await?
        {
            let existing = row_to_workspace(row);
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
            sqlx::query("UPDATE efforts SET workspace_id = ? WHERE workspace_id = ?")
                .bind(&workspace.id)
                .bind(&existing.id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM workspaces WHERE id = ?")
                .bind(&existing.id)
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
        node: &Node,
        revision: Option<&NodeRevision>,
        event: &AuditEvent,
        expected_version: i64,
    ) -> Result<i64, StoreError> {
        let mut tx = self.pool.begin().await?;
        check_version(&mut tx, &node.effort_id, expected_version).await?;
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

    pub async fn insert_claim(
        &self,
        claim: &Claim,
        now: &str,
        event: &AuditEvent,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("UPDATE claims SET released_at=?,release_reason='lease expired' WHERE node_id=? AND released_at IS NULL AND lease_expires_at<=?")
            .bind(now).bind(&claim.node_id).bind(now).execute(&mut *tx).await?;
        let result = sqlx::query("INSERT INTO claims(id,node_id,actor_id,session_id,claimed_at,heartbeat_at,lease_expires_at) VALUES(?,?,?,?,?,?,?)")
            .bind(&claim.id).bind(&claim.node_id).bind(&claim.actor_id).bind(&claim.session_id)
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
        .bind(now)
        .bind(&claim.node_id)
        .execute(&mut *tx)
        .await?;
        insert_event(&mut tx, event).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn release_claim(
        &self,
        claim_id: &str,
        now: &str,
        reason: &str,
        event: &AuditEvent,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT node_id FROM claims WHERE id=? AND released_at IS NULL")
            .bind(claim_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(StoreError::NotFound)?;
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
        session_id: &str,
        heartbeat: &str,
        expires: &str,
    ) -> Result<Claim, StoreError> {
        let result = sqlx::query("UPDATE claims SET heartbeat_at=?,lease_expires_at=? WHERE id=? AND session_id=? AND released_at IS NULL AND lease_expires_at>?")
            .bind(heartbeat).bind(expires).bind(claim_id).bind(session_id).bind(heartbeat)
            .execute(&self.pool).await?;
        if result.rows_affected() != 1 {
            return Err(StoreError::NotFound);
        }
        let row = sqlx::query("SELECT * FROM claims WHERE id=?")
            .bind(claim_id)
            .fetch_one(&self.pool)
            .await?;
        Ok(row_to_claim(row))
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
        preview: &InvalidationPreview,
        event: &AuditEvent,
        expected_version: i64,
        now: &str,
    ) -> Result<i64, StoreError> {
        let mut tx = self.pool.begin().await?;
        check_version(&mut tx, effort_id, expected_version).await?;
        for change in &preview.changes {
            sqlx::query("UPDATE nodes SET validity=?,updated_at=? WHERE id=? AND effort_id=?")
                .bind(change.to.as_str())
                .bind(now)
                .bind(&change.node_id)
                .bind(effort_id)
                .execute(&mut *tx)
                .await?;
        }
        for question in &preview.reopened_questions {
            sqlx::query("UPDATE nodes SET lifecycle='open',validity='review_required',updated_at=? WHERE id=? AND effort_id=?")
                .bind(now).bind(question).bind(effort_id).execute(&mut *tx).await?;
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

    pub async fn list_events(&self, effort_id: &str) -> Result<Vec<AuditEvent>, StoreError> {
        let rows = sqlx::query("SELECT * FROM events WHERE effort_id=? ORDER BY occurred_at,id")
            .bind(effort_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(row_to_event).collect()
    }

    pub async fn list_sources(&self, effort_id: &str) -> Result<Vec<Source>, StoreError> {
        let rows = sqlx::query("SELECT * FROM sources WHERE effort_id=? ORDER BY created_at")
            .bind(effort_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(row_to_source).collect()
    }
}

async fn check_version(
    tx: &mut Transaction<'_, Sqlite>,
    effort_id: &str,
    expected: i64,
) -> Result<(), StoreError> {
    let row = sqlx::query("SELECT version FROM efforts WHERE id=?")
        .bind(effort_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(StoreError::NotFound)?;
    let actual: i64 = row.get("version");
    if actual == expected {
        Ok(())
    } else {
        Err(StoreError::VersionConflict { expected, actual })
    }
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
    sqlx::query("INSERT INTO events(id,effort_id,actor_id,session_id,event_type,entity_type,entity_id,before_json,after_json,reason,occurred_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)")
        .bind(&event.id).bind(&event.effort_id).bind(&event.actor_id).bind(&event.session_id)
        .bind(&event.event_type).bind(&event.entity_type).bind(&event.entity_id)
        .bind(event.before.as_ref().map(|value| serde_json::to_string(value).expect("JSON value serializes")))
        .bind(event.after.as_ref().map(|value| serde_json::to_string(value).expect("JSON value serializes")))
        .bind(&event.reason).bind(&event.occurred_at).execute(&mut **tx).await?;
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
        session_id: row.get("session_id"),
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

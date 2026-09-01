use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use serde::Serialize;
use serde_json::{Value, json};
use threadmark_application::{AddEdge, AddNode, CreateEffort, Service};
use threadmark_domain::{
    Confidence, EdgeType, Lifecycle, NewEdge, NewNode, NodeKind, Reversibility, RiskLevel,
    SourceKind, SourceTrust, Uncertainty, Validity,
};
use threadmark_export::{PortableEffort, render_handoff, write_package};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "threadmark", version, about = "Durable reasoning graphs for humans and agents")]
struct Cli {
    #[arg(long, global = true, default_value = ".")]
    workspace: PathBuf,
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Init { #[arg(long)] name: String },
    #[command(subcommand)]
    Effort(EffortCommand),
    #[command(subcommand)]
    Node(NodeCommand),
    #[command(subcommand)]
    Edge(EdgeCommand),
    Status { effort: String },
    Frontier { effort: String },
    #[command(subcommand)]
    Claim(ClaimCommand),
    Resolve(ResolveArgs),
    Reopen {
        effort: String,
        node: String,
        #[arg(long, default_value = "human")]
        actor: String,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        expected_version: Option<i64>,
    },
    #[command(subcommand)]
    Invalidate(InvalidateCommand),
    #[command(subcommand)]
    Fog(FogCommand),
    #[command(subcommand)]
    Source(SourceCommand),
    Criterion {
        effort: String,
        criterion_type: String,
        #[arg(long, default_value = "{}")]
        config: String,
        #[arg(long, default_value_t = true)]
        required: bool,
        #[arg(long, default_value = "human")]
        actor: String,
        #[arg(long)]
        expected_version: Option<i64>,
    },
    Contradiction {
        effort: String,
        left: String,
        right: String,
        #[arg(long)]
        detail: String,
        #[arg(long, default_value = "high")]
        severity: RiskLevel,
        #[arg(long, default_value = "human")]
        actor: String,
        #[arg(long)]
        expected_version: Option<i64>,
    },
    History {
        effort: String,
        #[arg(long)]
        node: Option<String>,
    },
    Why { effort: String, node: String },
    Lint { effort: String },
    Readiness { effort: String },
    Export {
        effort: String,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        include_events: bool,
    },
    Handoff {
        effort: String,
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum EffortCommand {
    Create {
        slug: String,
        #[arg(long)]
        title: String,
        #[arg(long)]
        destination: String,
        #[arg(long, default_value = "")]
        scope_notes: String,
        #[arg(long, default_value = "human")]
        actor: String,
    },
    List,
    Show { effort: String },
}

#[derive(Debug, Subcommand)]
enum NodeCommand {
    Add(NodeAddArgs),
    Show { effort: String, node: String },
    List {
        effort: String,
        #[arg(long)]
        kind: Option<NodeKind>,
    },
}

#[derive(Debug, Args)]
struct NodeAddArgs {
    effort: String,
    kind: NodeKind,
    #[arg(long)]
    title: String,
    #[arg(long, default_value = "")]
    summary: String,
    #[arg(long, default_value = "")]
    body: String,
    #[arg(long, default_value = "{}")]
    payload: String,
    #[arg(long, default_value = "open")]
    lifecycle: Lifecycle,
    #[arg(long)]
    confidence: Option<Confidence>,
    #[arg(long)]
    confidence_reason: Option<String>,
    #[arg(long)]
    reversibility: Option<Reversibility>,
    #[arg(long)]
    impact: Option<RiskLevel>,
    #[arg(long)]
    uncertainty: Option<Uncertainty>,
    #[arg(long)]
    cost_of_wrong: Option<RiskLevel>,
    #[arg(long, default_value = "human")]
    actor: String,
    #[arg(long)]
    session: Option<String>,
    #[arg(long)]
    expected_version: Option<i64>,
}

#[derive(Debug, Subcommand)]
enum EdgeCommand {
    Add {
        effort: String,
        source: String,
        edge_type: EdgeType,
        target: String,
        #[arg(long)]
        rationale: Option<String>,
        #[arg(long, default_value = "human")]
        actor: String,
        #[arg(long)]
        expected_version: Option<i64>,
    },
    List { effort: String },
}

#[derive(Debug, Subcommand)]
enum ClaimCommand {
    Next(ClaimArgs),
    Node {
        #[command(flatten)]
        claim: ClaimArgs,
        node: String,
    },
    Release {
        effort: String,
        claim: String,
        #[arg(long, default_value = "human")]
        actor: String,
        #[arg(long, default_value = "released")]
        reason: String,
    },
    Heartbeat {
        claim: String,
        #[arg(long)]
        session: String,
        #[arg(long, default_value_t = 30)]
        lease_minutes: i64,
    },
    List { effort: String },
}

#[derive(Debug, Args)]
struct ClaimArgs {
    effort: String,
    #[arg(long)]
    actor: String,
    #[arg(long)]
    session: String,
    #[arg(long, default_value_t = 30)]
    lease_minutes: i64,
}

#[derive(Debug, Args)]
struct ResolveArgs {
    effort: String,
    node: String,
    #[arg(long, default_value = "human")]
    actor: String,
    #[arg(long)]
    session: Option<String>,
    #[arg(long)]
    body: String,
    #[arg(long)]
    payload: Option<String>,
    #[arg(long)]
    confidence: Option<Confidence>,
    #[arg(long)]
    confidence_reason: Option<String>,
    #[arg(long, default_value = "resolved")]
    reason: String,
    #[arg(long)]
    expected_version: Option<i64>,
}

#[derive(Debug, Subcommand)]
enum InvalidateCommand {
    Preview {
        effort: String,
        node: String,
        #[arg(long, default_value = "invalid")]
        target: Validity,
    },
    Commit {
        effort: String,
        node: String,
        #[arg(long, default_value = "invalid")]
        target: Validity,
        #[arg(long, default_value = "human")]
        actor: String,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        expected_version: Option<i64>,
    },
}

#[derive(Debug, Subcommand)]
enum FogCommand {
    Add {
        effort: String,
        #[arg(long)]
        title: String,
        #[arg(long)]
        description: String,
        #[arg(long)]
        anchor: Option<String>,
        #[arg(long, default_value = "human")]
        actor: String,
        #[arg(long)]
        expected_version: Option<i64>,
    },
    Graduate {
        effort: String,
        fog: String,
        #[arg(long, required = true)]
        to: Vec<String>,
        #[arg(long, default_value = "human")]
        actor: String,
        #[arg(long)]
        expected_version: Option<i64>,
    },
    List { effort: String },
}

#[derive(Debug, Subcommand)]
enum SourceCommand {
    Add {
        effort: String,
        kind: SourceKind,
        #[arg(long)]
        title: String,
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        excerpt: Option<String>,
        #[arg(long, default_value = "unreviewed")]
        trust: SourceTrust,
        #[arg(long, default_value = "human")]
        actor: String,
        #[arg(long)]
        expected_version: Option<i64>,
    },
    Attach {
        effort: String,
        node: String,
        source: String,
        #[arg(long, default_value = "supports")]
        relationship: String,
        #[arg(long, default_value = "human")]
        actor: String,
        #[arg(long)]
        expected_version: Option<i64>,
    },
    List { effort: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    if let Command::Init { name } = &cli.command {
        let service = Service::init(&cli.workspace, name).await?;
        return output(cli.json, service.workspace(), || format!("Initialized Threadmark workspace {} ({})", service.workspace().name, service.workspace().id));
    }
    let service = Service::open(&cli.workspace).await?;
    dispatch(&service, cli.command, cli.json).await
}

async fn dispatch(service: &Service, command: Command, json_output: bool) -> Result<()> {
    match command {
        Command::Init { .. } => unreachable!(),
        Command::Effort(command) => match command {
            EffortCommand::Create { slug, title, destination, scope_notes, actor } => {
                let effort = service.create_effort(CreateEffort { slug, title, destination, scope_notes, actor_id: actor }).await?;
                output(json_output, &effort, || format!("Created effort {} ({})", effort.title, effort.id))
            }
            EffortCommand::List => {
                let efforts = service.list_efforts().await?;
                output(json_output, &efforts, || efforts.iter().map(|effort| format!("{}\t{}\t{}\tv{}", effort.id, effort.slug, effort.status, effort.version)).collect::<Vec<_>>().join("\n"))
            }
            EffortCommand::Show { effort } => {
                let effort = service.get_effort(&effort).await?;
                output(json_output, &effort, || format!("{} ({})\nDestination: {}\nStatus: {}\nVersion: {}", effort.title, effort.id, effort.destination, effort.status, effort.version))
            }
        },
        Command::Node(command) => match command {
            NodeCommand::Add(args) => add_node(service, args, json_output).await,
            NodeCommand::Show { effort, node } => {
                let node = service.get_node(&effort, &node).await?;
                output(json_output, &node, || format!("{} ({})\n{} / {}\n{}", node.title, node.id, node.lifecycle, node.validity, node.body))
            }
            NodeCommand::List { effort, kind } => {
                let (_, graph) = service.snapshot(&effort).await?;
                let nodes: Vec<_> = graph.nodes.into_iter().filter(|node| kind.is_none_or(|kind| node.kind == kind)).collect();
                output(json_output, &nodes, || nodes.iter().map(|node| format!("{}\t{}\t{}\t{}", node.id, node.kind, node.lifecycle, node.title)).collect::<Vec<_>>().join("\n"))
            }
        },
        Command::Edge(command) => match command {
            EdgeCommand::Add { effort, source, edge_type, target, rationale, actor, expected_version } => {
                let (edge, version) = service.add_edge(AddEdge { effort, edge: NewEdge { source_node_id: source, edge_type, target_node_id: target, rationale }, actor_id: actor, expected_version }).await?;
                output(json_output, &json!({"edge":edge,"effort_version":version}), || format!("Added {} edge {} -> {} (v{version})", edge.edge_type, edge.source_node_id, edge.target_node_id))
            }
            EdgeCommand::List { effort } => {
                let (_, graph) = service.snapshot(&effort).await?;
                output(json_output, &graph.edges, || graph.edges.iter().map(|edge| format!("{}\t{} {} {}", edge.id, edge.source_node_id, edge.edge_type, edge.target_node_id)).collect::<Vec<_>>().join("\n"))
            }
        },
        Command::Status { effort } => {
            let status = service.status(&effort).await?;
            output(json_output, &status, || render_status(&status))
        }
        Command::Frontier { effort } => {
            let status = service.status(&effort).await?;
            output(json_output, &status.frontier, || status.frontier.iter().enumerate().map(|(index, entry)| format!("{}. {} ({}) — {}", index + 1, entry.node.title, entry.node.id, entry.explanation)).collect::<Vec<_>>().join("\n"))
        }
        Command::Claim(command) => match command {
            ClaimCommand::Next(args) => {
                let claim = service.claim_next(&args.effort, &args.actor, &args.session, args.lease_minutes).await?;
                output(json_output, &claim, || format!("Claimed {} until {} ({})", claim.node_id, claim.lease_expires_at, claim.id))
            }
            ClaimCommand::Node { claim: args, node } => {
                let claim = service.claim_node(&args.effort, &node, &args.actor, &args.session, args.lease_minutes).await?;
                output(json_output, &claim, || format!("Claimed {} until {} ({})", claim.node_id, claim.lease_expires_at, claim.id))
            }
            ClaimCommand::Release { effort, claim, actor, reason } => {
                service.release_claim(&effort, &claim, &actor, &reason).await?;
                println!("Released claim {claim}");
                Ok(())
            }
            ClaimCommand::Heartbeat { claim, session, lease_minutes } => {
                let claim = service.heartbeat_claim(&claim, &session, lease_minutes).await?;
                output(json_output, &claim, || format!("Extended claim {} until {}", claim.id, claim.lease_expires_at))
            }
            ClaimCommand::List { effort } => {
                let (_, graph) = service.snapshot(&effort).await?;
                output(json_output, &graph.claims, || graph.claims.iter().map(|claim| format!("{}\t{}\t{}\t{}", claim.id, claim.node_id, claim.actor_id, claim.lease_expires_at)).collect::<Vec<_>>().join("\n"))
            }
        },
        Command::Resolve(args) => {
            let payload = args.payload.map(|payload| serde_json::from_str(&payload)).transpose().context("invalid payload JSON")?;
            let (node, version) = service.resolve_node(&args.effort, &args.node, &args.actor, args.session.as_deref(), args.body, payload, args.confidence, args.confidence_reason, &args.reason, args.expected_version).await?;
            output(json_output, &json!({"node":node,"effort_version":version}), || format!("Resolved {} ({}) at v{version}", node.title, node.id))
        }
        Command::Reopen { effort, node, actor, reason, expected_version } => {
            let (node, version) = service.reopen_node(&effort, &node, &actor, &reason, expected_version).await?;
            output(json_output, &json!({"node":node,"effort_version":version}), || format!("Reopened {} ({}) at v{version}", node.title, node.id))
        }
        Command::Invalidate(command) => match command {
            InvalidateCommand::Preview { effort, node, target } => {
                let preview = service.invalidation_preview(&effort, &node, target).await?;
                output(json_output, &preview, || render_invalidation(&preview))
            }
            InvalidateCommand::Commit { effort, node, target, actor, reason, expected_version } => {
                let (preview, version) = service.commit_invalidation(&effort, &node, target, &actor, &reason, expected_version).await?;
                output(json_output, &json!({"preview":preview,"effort_version":version}), || format!("{}\nCommitted at v{version}", render_invalidation(&preview)))
            }
        },
        Command::Fog(command) => match command {
            FogCommand::Add { effort, title, description, anchor, actor, expected_version } => {
                let (fog, version) = service.add_fog(&effort, title, description, anchor, &actor, expected_version).await?;
                output(json_output, &json!({"fog":fog,"effort_version":version}), || format!("Added fog patch {} ({}) at v{version}", fog.title, fog.id))
            }
            FogCommand::Graduate { effort, fog, to, actor, expected_version } => {
                let version = service.graduate_fog(&effort, &fog, &to, &actor, expected_version).await?;
                output(json_output, &json!({"fog":fog,"graduated_to":to,"effort_version":version}), || format!("Graduated fog patch {fog} at v{version}"))
            }
            FogCommand::List { effort } => {
                let (_, graph) = service.snapshot(&effort).await?;
                output(json_output, &graph.fog_patches, || graph.fog_patches.iter().map(|fog| format!("{}\t{}\t{}", fog.id, fog.status, fog.title)).collect::<Vec<_>>().join("\n"))
            }
        },
        Command::Source(command) => match command {
            SourceCommand::Add { effort, kind, title, uri, excerpt, trust, actor, expected_version } => {
                let (source, version) = service.add_source(&effort, kind, title, uri, excerpt, trust, &actor, expected_version).await?;
                output(json_output, &json!({"source":source,"effort_version":version}), || format!("Added source {} ({}) at v{version}", source.title, source.id))
            }
            SourceCommand::Attach { effort, node, source, relationship, actor, expected_version } => {
                let version = service.attach_source(&effort, &node, &source, &relationship, &actor, expected_version).await?;
                output(json_output, &json!({"node":node,"source":source,"relationship":relationship,"effort_version":version}), || format!("Attached source {source} to {node} at v{version}"))
            }
            SourceCommand::List { effort } => {
                let sources = service.sources(&effort).await?;
                output(json_output, &sources, || sources.iter().map(|source| format!("{}\t{}\t{}\t{}", source.id, source.kind, source.trust, source.title)).collect::<Vec<_>>().join("\n"))
            }
        },
        Command::Criterion { effort, criterion_type, config, required, actor, expected_version } => {
            let config = serde_json::from_str(&config).context("invalid criterion config JSON")?;
            let (criterion, version) = service.add_exit_criterion(&effort, criterion_type, config, required, &actor, expected_version).await?;
            output(json_output, &json!({"criterion":criterion,"effort_version":version}), || format!("Added criterion {} at v{version}", criterion.criterion_type))
        }
        Command::Contradiction { effort, left, right, detail, severity, actor, expected_version } => {
            let (finding, version) = service.propose_contradiction(&effort, &left, &right, detail, severity, &actor, expected_version).await?;
            output(json_output, &json!({"finding":finding,"effort_version":version}), || format!("Proposed contradiction {} at v{version}", finding.id))
        }
        Command::History { effort, node } => {
            if let Some(node) = node {
                let revisions = service.node_history(&effort, &node).await?;
                output(json_output, &revisions, || revisions.iter().map(|revision| format!("v{}\t{}\t{}", revision.revision, revision.created_at, revision.reason.as_deref().unwrap_or(""))).collect::<Vec<_>>().join("\n"))
            } else {
                let events = service.effort_history(&effort).await?;
                output(json_output, &events, || events.iter().map(|event| format!("{}\t{}\t{}\t{}", event.occurred_at, event.event_type, event.entity_type, event.entity_id)).collect::<Vec<_>>().join("\n"))
            }
        }
        Command::Why { effort, node } => {
            let node = service.get_node(&effort, &node).await?;
            let revisions = service.node_history(&effort, &node.id).await?;
            let (_, graph) = service.snapshot(&effort).await?;
            let edges: Vec<_> = graph.edges.into_iter().filter(|edge| edge.source_node_id == node.id || edge.target_node_id == node.id).collect();
            let value = json!({"node":node,"relationships":edges,"revisions":revisions});
            output(json_output, &value, || format!("{} ({})\n{} / {}\n\n{}\n\nRelationships:\n{}\n\nRevisions: {}", node.title, node.id, node.lifecycle, node.validity, node.body, edges.iter().map(|edge| format!("- {} {} {}", edge.source_node_id, edge.edge_type, edge.target_node_id)).collect::<Vec<_>>().join("\n"), revisions.len()))
        }
        Command::Lint { effort } => {
            let status = service.status(&effort).await?;
            output(json_output, &status.lint, || if status.lint.is_empty() { "No lint findings.".into() } else { status.lint.iter().map(|finding| format!("{:?} {}: {}", finding.severity, finding.code, finding.message)).collect::<Vec<_>>().join("\n") })
        }
        Command::Readiness { effort } => {
            let status = service.status(&effort).await?;
            output(json_output, &status.readiness, || render_readiness(&status.readiness))
        }
        Command::Export { effort, output: directory, include_events } => {
            let package = package(service, &effort, include_events).await?;
            write_package(&package, &directory, include_events)?;
            println!("Exported {} to {}", package.effort.title, directory.display());
            Ok(())
        }
        Command::Handoff { effort, output: path } => {
            let package = package(service, &effort, false).await?;
            let handoff = render_handoff(&package);
            if let Some(path) = path { std::fs::write(&path, handoff)?; println!("Wrote {}", path.display()); }
            else { print!("{handoff}"); }
            Ok(())
        }
    }
}

async fn add_node(service: &Service, args: NodeAddArgs, json_output: bool) -> Result<()> {
    let payload: Value = serde_json::from_str(&args.payload).context("invalid payload JSON")?;
    let (node, version) = service.add_node(AddNode { effort: args.effort, node: NewNode {
        kind: args.kind, title: args.title, summary: args.summary, body: args.body, payload, lifecycle: args.lifecycle,
        confidence: args.confidence, confidence_reason: args.confidence_reason, reversibility: args.reversibility,
        impact: args.impact, uncertainty: args.uncertainty, cost_of_wrong: args.cost_of_wrong,
    }, actor_id: args.actor, session_id: args.session, expected_version: args.expected_version }).await?;
    output(json_output, &json!({"node":node,"effort_version":version}), || format!("Added {} {} ({}) at v{version}", node.kind, node.title, node.id))
}

async fn package(service: &Service, effort: &str, include_events: bool) -> Result<PortableEffort> {
    let (effort_value, graph) = service.snapshot(effort).await?;
    let sources = service.sources(effort).await?;
    let events = if include_events { service.effort_history(effort).await? } else { vec![] };
    Ok(PortableEffort { format_version: 1, effort: effort_value, graph, sources, events })
}

fn output<T: Serialize>(json_output: bool, value: &T, human: impl FnOnce() -> String) -> Result<()> {
    if json_output { println!("{}", serde_json::to_string_pretty(value)?); } else { println!("{}", human()); }
    Ok(())
}

fn render_status(status: &threadmark_application::EffortStatusView) -> String {
    format!("{} — {}\nReadiness: {}\nVersion: {}\nFrontier: {}\nActive fog: {}\nActive findings: {}\nLint findings: {}",
        status.effort.title, status.effort.destination, if status.readiness.ready { "READY" } else { "NOT READY" },
        status.effort.version, status.frontier.len(), status.active_fog, status.active_findings, status.lint.len())
}

fn render_readiness(readiness: &threadmark_domain::ReadinessReport) -> String {
    let mut lines = vec![format!("Readiness: {}", if readiness.ready { "READY" } else { "NOT READY" })];
    lines.extend(readiness.results.iter().map(|result| format!("{} {} — {}", if result.passed { "✓" } else { "✗" }, result.criterion_type, result.explanation)));
    lines.join("\n")
}

fn render_invalidation(preview: &threadmark_domain::InvalidationPreview) -> String {
    if preview.changes.is_empty() { return "No changes.".into(); }
    let mut lines = preview.changes.iter().map(|change| format!("{}: {} -> {} ({})", change.node_id, change.from, change.to, change.reason)).collect::<Vec<_>>();
    for question in &preview.reopened_questions { lines.push(format!("reopen question: {question}")); }
    lines.join("\n")
}

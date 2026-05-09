use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::response::Response;
use axum::routing::get;
use axum::{Json, Router};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use gpui::App;
use log::{error, info};
use serde::Serialize;
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};

use terminal::LocalApiRegistry;

// ─── App state ───────────────────────────────────────────────────────────────

#[derive(Clone)]
struct ApiState {
    registry: LocalApiRegistry,
}

// ─── JSON types ──────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct TerminalInfo {
    id: u64,
}

// ─── Initialisation ──────────────────────────────────────────────────────────

/// Start the local API server if the `terminal.local_api.enabled` setting is
/// true.  Registers the [`LocalApiRegistry`] as a GPUI global.  Call once
/// during app init (after settings are loaded).
pub fn init(cx: &mut App) {
    use settings::Settings as _;
    use terminal::terminal_settings::TerminalSettings;

    let local_api = &TerminalSettings::get_global(cx).local_api;
    if !local_api.enabled {
        return;
    }
    let port = local_api.port;

    let registry = LocalApiRegistry::new();
    // Clone before moving into the GPUI global so the Axum server can keep its
    // own reference without needing GPUI context.
    let registry_for_server = registry.clone();
    cx.set_global(registry);

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("failed to build local-API tokio runtime");

        rt.block_on(async move {
            let state = ApiState {
                registry: registry_for_server,
            };
            let cors = CorsLayer::new().allow_origin(Any).allow_methods(Any);

            let app = Router::new()
                .route("/terminals", get(list_terminals))
                .route("/terminals/:id", get(terminal_ws))
                .with_state(state)
                .layer(cors);

            let addr: SocketAddr = format!("127.0.0.1:{}", port)
                .parse()
                .expect("invalid local-API address");

            info!("zed_local_api: listening on {addr}");

            if let Err(err) = axum::Server::bind(&addr)
                .serve(app.into_make_service())
                .await
            {
                error!("zed_local_api: server error: {err}");
            }
        });
    });
}

// ─── Handlers ────────────────────────────────────────────────────────────────

async fn list_terminals(State(state): State<ApiState>) -> Json<Vec<TerminalInfo>> {
    let entries = state.registry.lock_entries();
    let list = entries
        .values()
        .filter(|e| !e.input_tx.is_closed())
        .map(|e| TerminalInfo { id: e.id })
        .collect::<Vec<_>>();
    Json(list)
}

async fn terminal_ws(
    Path(id): Path<u64>,
    State(state): State<ApiState>,
    upgrade: WebSocketUpgrade,
) -> Response {
    upgrade.on_upgrade(move |socket| handle_terminal_socket(id, state, socket))
}

async fn handle_terminal_socket(id: u64, state: ApiState, socket: WebSocket) {
    // Resolve entry handles while holding the lock as briefly as possible.
    let (output_tx, snapshot_arc, input_tx) = {
        let entries = state.registry.lock_entries();
        let Some(entry) = entries.get(&id) else {
            return;
        };
        (
            Arc::clone(&entry.output_tx),
            entry.snapshot.clone(),
            entry.input_tx.clone(),
        )
    };

    // Subscribe before reading the snapshot so we don't miss frames that
    // arrive between the snapshot read and the subscribe call.
    let mut output_rx: broadcast::Receiver<Bytes> = output_tx.subscribe();

    let snapshot: Bytes = snapshot_arc
        .read()
        .map(|g| g.clone())
        .unwrap_or_default();

    let (mut ws_tx, mut ws_rx) = socket.split();

    if !snapshot.is_empty() {
        if ws_tx
            .send(Message::Binary(snapshot.to_vec()))
            .await
            .is_err()
        {
            return;
        }
    }

    loop {
        tokio::select! {
            // Outbound: terminal output → WebSocket client
            result = output_rx.recv() => {
                match result {
                    Ok(frame) => {
                        if ws_tx.send(Message::Binary(frame.to_vec())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Missed some frames – resync with a fresh snapshot.
                        let fresh: Bytes = snapshot_arc
                            .read()
                            .map(|g| g.clone())
                            .unwrap_or_default();
                        if !fresh.is_empty()
                            && ws_tx.send(Message::Binary(fresh.to_vec())).await.is_err()
                        {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            // Inbound: WebSocket client → PTY input
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        let _ = input_tx.unbounded_send(Bytes::from(data));
                    }
                    Some(Ok(Message::Text(text))) => {
                        let _ = input_tx.unbounded_send(Bytes::from(text.into_bytes()));
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }
}

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use gpui::App;
use log::{error, info};
use serde::Serialize;
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};

use terminal::{LocalApiBufferRegistry, LocalApiInput, LocalApiRegistry};

// App state.

#[derive(Clone)]
struct ApiState {
    registry: LocalApiRegistry,
    buffer_registry: LocalApiBufferRegistry,
}

// JSON types.

#[derive(Serialize)]
struct TerminalInfo {
    id: u64,
    title: String,
    rows: usize,
    cols: usize,
    is_active: bool,
}

#[derive(Serialize)]
struct BufferInfo {
    path: String,
    language: Option<String>,
    is_dirty: bool,
}

// Initialisation.

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
    let buffer_registry = LocalApiBufferRegistry::new();
    // Clone before moving into the GPUI global so the Axum server can keep its
    // own reference without needing GPUI context.
    let registry_for_server = registry.clone();
    let buffer_registry_for_server = buffer_registry.clone();
    cx.set_global(registry);
    cx.set_global(buffer_registry);

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("failed to build local-API tokio runtime");

        rt.block_on(async move {
            let state = ApiState {
                registry: registry_for_server,
                buffer_registry: buffer_registry_for_server,
            };
            let cors = CorsLayer::new().allow_origin(Any).allow_methods(Any);

            let app = Router::new()
                .route("/terminals", get(list_terminals))
                .route("/terminals/:id", get(terminal_ws))
                .route("/buffers", get(list_buffers))
                .route("/buffers/*path", get(read_buffer))
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

// Handlers.

async fn list_terminals(State(state): State<ApiState>) -> Json<Vec<TerminalInfo>> {
    let entries = state.registry.lock_entries();
    let list = entries
        .values()
        .filter(|e| !e.input_tx.is_closed())
        .map(|e| TerminalInfo {
            id: e.id,
            title: e
                .title
                .read()
                .map(|title| title.clone())
                .unwrap_or_default(),
            rows: e.rows.load(std::sync::atomic::Ordering::Relaxed),
            cols: e.cols.load(std::sync::atomic::Ordering::Relaxed),
            is_active: e.is_active.load(std::sync::atomic::Ordering::Relaxed),
        })
        .collect::<Vec<_>>();
    Json(list)
}

async fn list_buffers(State(state): State<ApiState>) -> Json<Vec<BufferInfo>> {
    Json(
        state
            .buffer_registry
            .list_buffers()
            .into_iter()
            .map(|buffer| BufferInfo {
                path: buffer.path,
                language: buffer.language,
                is_dirty: buffer.is_dirty,
            })
            .collect(),
    )
}

async fn read_buffer(Path(path): Path<String>, State(state): State<ApiState>) -> impl IntoResponse {
    let path = if path.starts_with('/') {
        path
    } else {
        format!("/{path}")
    };

    match state.buffer_registry.get_buffer_by_path(&path) {
        Some(buffer) => (StatusCode::OK, buffer.content).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
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

    let snapshot: Bytes = snapshot_arc.read().map(|g| g.clone()).unwrap_or_default();

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
            // Outbound: terminal output to WebSocket client.
            result = output_rx.recv() => {
                match result {
                    Ok(frame) => {
                        if ws_tx.send(Message::Binary(frame.to_vec())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Missed some frames, so resync with a fresh snapshot.
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
            // Inbound: WebSocket client to PTY input.
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        if let Err(err) = input_tx.unbounded_send(LocalApiInput::Bytes(Bytes::from(data))) {
                            error!("zed_local_api: failed to forward terminal input: {err}");
                            break;
                        }
                    }
                    Some(Ok(Message::Text(text))) => {
                        if let Some((rows, cols)) = parse_resize_message(&text) {
                            if let Err(err) = input_tx.unbounded_send(LocalApiInput::Resize { rows, cols }) {
                                error!("zed_local_api: failed to forward terminal resize: {err}");
                                break;
                            }
                        } else if let Err(err) = input_tx.unbounded_send(LocalApiInput::Bytes(Bytes::from(text.into_bytes()))) {
                            error!("zed_local_api: failed to forward terminal text input: {err}");
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }
}

fn parse_resize_message(text: &str) -> Option<(u16, u16)> {
    #[derive(serde::Deserialize)]
    struct ResizeMessage {
        #[serde(rename = "type")]
        message_type: String,
        rows: u16,
        cols: u16,
    }

    let message: ResizeMessage = serde_json::from_str(text).ok()?;
    if message.message_type == "resize" && message.rows > 0 && message.cols > 0 {
        Some((message.rows, message.cols))
    } else {
        None
    }
}

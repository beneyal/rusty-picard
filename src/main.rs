use api::{
    BatchFeedResult, BatchParseRequest, FeedResult, ServerState, ValidationRequest,
    ValidationResult,
};
use axum::{
    response::IntoResponse,
    routing::{get, post},
    Extension, Json, Router,
};
use domain::{QplEnvironment, QplState, SqlSchema};
use parser::{api::prefixed_qpl, shared::Stream};
use rayon::prelude::*;
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokenizers::Tokenizer;
use tokio::sync::RwLock;
use tracing::debug;
use winnow::{error::ErrMode, stream::StreamIsPartial, Parser, Partial};

mod api;
pub(crate) mod domain;
mod parser;
mod schemas;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let app = Router::new()
        .route("/health", get(health))
        .route("/debug", get(log_state))
        .route("/schema", post(register_schema))
        .route("/tokenizer", post(register_tokenizer))
        .route("/validate", post(validate_qpl))
        .route("/parse", post(parse_qpl))
        .layer(Extension(SharedState::default()));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8081").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

type SharedState = Arc<RwLock<ServerState>>;

async fn health() {}

async fn log_state(Extension(state): Extension<SharedState>) {
    debug!(?state)
}

async fn register_schema(Extension(state): Extension<SharedState>, Json(schema): Json<SqlSchema>) {
    let mut state = state.write().await;
    debug!("Added schema {}", schema.db_id);
    state.schemas.insert(schema.db_id.clone(), schema);
}

async fn register_tokenizer(Extension(state): Extension<SharedState>, tokenizer_repr: String) {
    let state = state.write().await;
    let tokenizer = Tokenizer::from_str(&tokenizer_repr).unwrap();
    debug!("Setting tokenizer");
    let mut mutex = state.tokenizer.lock().unwrap();
    *mutex = Some(tokenizer);
}

async fn validate_qpl(
    Extension(state): Extension<SharedState>,
    Json(req): Json<ValidationRequest>,
) -> impl IntoResponse {
    let state = state.read().await;
    let schemas = &state.schemas;
    let with_type_checking = state.with_type_checking;
    let mut input = Stream {
        input: Partial::new(&req.qpl),
        state: QplEnvironment {
            state: QplState::default(),
            schema: None,
        },
    };
    let _ = input.complete();
    let result = prefixed_qpl::<()>(schemas, with_type_checking).parse_next(&mut input);
    let response = match result {
        Ok(_) => ValidationResult::Valid,
        Err(_) => ValidationResult::Invalid {
            reason: "Failed to parse".to_owned(),
        },
    };
    Json(response)
}

async fn parse_qpl(
    Extension(state): Extension<SharedState>,
    Json(req): Json<BatchParseRequest>,
) -> Result<Json<Vec<BatchFeedResult>>, String> {
    let state = state.read().await;
    {
        let tokenizer = state.tokenizer.lock().unwrap();
        if tokenizer.is_none() {
            return Err("Tokenizer not registered".into());
        }
    }
    let result = batch_feed(&req.input_ids, &req.top_tokens, &state);
    Ok(Json(result))
}

fn batch_feed(
    input_ids: &[Vec<u32>],
    top_tokens: &[Vec<u32>],
    state: &ServerState,
) -> Vec<BatchFeedResult> {
    let triplets = top_tokens
        .iter()
        .zip(input_ids.iter())
        .zip(0..)
        .flat_map(|((tokens, inputs), batch_id)| tokens.iter().map(move |t| (batch_id, inputs, *t)))
        .collect::<Vec<_>>();

    let mut result = Vec::with_capacity(triplets.len());
    triplets
        .into_par_iter()
        .map(|(batch_id, input_ids, top_token)| {
            let feed_result = feed(input_ids, top_token, state);
            BatchFeedResult {
                batch_id,
                top_token,
                feed_result,
            }
        })
        .collect_into_vec(&mut result);

    result
}

fn feed(input_ids: &[u32], token: u32, state: &ServerState) -> FeedResult {
    let ServerState {
        tokenizer,
        schemas,
        with_type_checking,
    } = state;

    let mut tokenizer_input = Vec::from(input_ids);
    tokenizer_input.push(token);

    let decoded = detokenize(&tokenizer_input, tokenizer);

    let mut parser_input = Stream {
        input: Partial::new(decoded.strip_suffix("</s>").unwrap_or(&decoded)),
        state: QplEnvironment {
            state: QplState::default(),
            schema: None,
        },
    };

    if decoded.ends_with("</s>") {
        let _ = parser_input.complete();
    }

    match prefixed_qpl::<()>(schemas, *with_type_checking).parse_next(&mut parser_input) {
        Ok(_) => FeedResult::Complete,
        Err(ErrMode::Incomplete(_)) => FeedResult::Partial,
        Err(_) => FeedResult::Failure,
    }
}

fn detokenize(input_ids: &[u32], tokenizer: &Arc<Mutex<Option<Tokenizer>>>) -> String {
    tokenizer
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .decode(input_ids, false)
        .unwrap()
}

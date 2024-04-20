use api::{BatchFeedResult, FeedResult, ServerState, ValidationRequest, ValidationResult};
use axum::{
    http::request,
    response::IntoResponse,
    routing::{get, post},
    Extension, Json, Router,
};
use axum_macros::debug_handler;
use domain::{QplEnvironment, QplState, SqlSchema};
use parser::{api::prefixed_qpl, shared::Stream};
use std::{collections::HashMap, str::FromStr, sync::Arc};
use tokenizers::Tokenizer;
use tokio::sync::RwLock;
use tracing::debug;
use winnow::{
    ascii::{multispace0, Caseless},
    combinator::{alt, fail, opt},
    error::{ContextError, ErrMode, ErrorKind, InputError, ParserError, TreeError},
    stream::StreamIsPartial,
    PResult, Parser, Partial,
};

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
    let mut state = state.write().await;
    let tokenizer = Tokenizer::from_str(&tokenizer_repr).unwrap();
    debug!("Setting tokenizer");
    state.tokenizer = Some(tokenizer);
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
    let result = prefixed_qpl::<ContextError>(schemas, with_type_checking)
        .complete_err()
        .parse_next(&mut input);
    debug!(?result);
    let response = match result {
        Ok(_) => ValidationResult::Valid,
        Err(ErrMode::Incomplete(_)) => ValidationResult::Invalid {
            reason: "Partial result".to_owned(),
        },
        Err(_) => ValidationResult::Invalid {
            reason: "Failed to parse".to_owned(),
        },
    };
    Json(response)
}

async fn parse_qpl() {}

async fn batch_feed(input_ids: &[&[usize]], top_tokens: &[&[usize]]) -> Vec<BatchFeedResult> {
    let triplets = top_tokens.iter().zip(input_ids.iter()).zip(0..).flat_map(
        |((tokens, inputs), batch_id)| tokens.iter().map(move |t| (batch_id, inputs, *t)),
    );

    todo!()
}

async fn feed(input_ids: &[usize], token: usize) -> FeedResult {
    todo!()
}

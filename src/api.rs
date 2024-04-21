use crate::domain::SqlSchema;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokenizers::Tokenizer;

#[derive(Debug, Default)]
pub(crate) struct ServerState {
    // pub(crate) counter: usize,
    pub(crate) tokenizer: Arc<Mutex<Option<Tokenizer>>>,
    pub(crate) schemas: HashMap<String, SqlSchema>,
    // pub(crate) partial_parses: HashMap<Vec<u32>, PartialParse>,
    pub(crate) with_type_checking: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct BatchParseRequest {
    pub(crate) input_ids: Vec<Vec<u32>>,
    pub(crate) top_tokens: Vec<Vec<u32>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct BatchFeedResult {
    pub(crate) batch_id: u32,
    pub(crate) top_token: u32,
    pub(crate) feed_result: FeedResult,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase", tag = "tag")]
pub(crate) enum FeedResult {
    Complete,
    Partial,
    Failure,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ValidationRequest {
    pub(crate) qpl: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase", tag = "tag")]
pub(crate) enum ValidationResult {
    Valid,
    Invalid { reason: String },
}

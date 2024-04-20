use crate::domain::{Qpl, SqlSchema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokenizers::Tokenizer;
use winnow::PResult;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct BatchFeedResult {
    batch_id: usize,
    top_token: usize,
    feed_result: FeedResult,
}

impl BatchFeedResult {
    pub(crate) fn new(batch_id: usize, top_token: usize, feed_result: FeedResult) -> Self {
        Self {
            batch_id,
            top_token,
            feed_result,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase", tag = "tag")]
pub(crate) enum FeedResult {
    Complete,
    Partial,
    Failure { message: String },
}

#[derive(Debug, Default)]
pub(crate) struct ServerState {
    pub(crate) counter: usize,
    pub(crate) tokenizer: Option<Tokenizer>,
    pub(crate) schemas: HashMap<String, SqlSchema>,
    pub(crate) partial_parses: HashMap<Vec<usize>, PartialParse>,
    pub(crate) with_type_checking: bool,
}

#[derive(Debug)]
pub(crate) struct PartialParse {
    decoded_input_ids: String,
    result: PResult<Qpl>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ValidationRequest {
    pub(crate) qpl: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase", tag = "tag")]
pub(crate) enum ValidationResult {
    Invalid { reason: String },
    Valid,
}

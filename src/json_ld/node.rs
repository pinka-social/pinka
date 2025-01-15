use std::collections::BTreeMap;

use anyhow::Result;
use serde_json::Number;

use super::vocab::TermAtom;

pub(crate) struct Node {
    pub(crate) properties: BTreeMap<TermAtom, Value>,
}

pub(crate) enum Value {
    Null,
    Bool(bool),
    Number(Number),
    String(String),
    Array(Vec<Value>),
    Node(BTreeMap<TermAtom, Value>),
}

impl Node {
    pub(crate) fn from_str(s: &str) -> Result<Node> {
        let value: serde_json::Value = serde_json::from_str(s)?;
        todo!()
    }
}

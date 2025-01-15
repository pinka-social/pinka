//! Just enough JSON-LD

use anyhow::Result;
use fjall::{Keyspace, PartitionHandle};

use self::vocab::{Term, TermAtom};

mod context;
mod node;
mod vocab;

pub(crate) struct System {
    keyspace: Keyspace,
    terms: PartitionHandle,
    nodes: PartitionHandle,
}

impl System {
    pub(crate) fn intern(term: &Term) -> Result<TermAtom> {
        todo!()
    }
    pub(crate) fn get_term(atom: TermAtom) -> Result<Term> {
        todo!()
    }
}

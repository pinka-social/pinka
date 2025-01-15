use std::borrow::Cow;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TermAtom(u32);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Term {
    Iri(Cow<'static, str>),
    Keyword(Cow<'static, str>),
}

impl Default for Term {
    fn default() -> Self {
        Term::Iri(Cow::default())
    }
}

impl Term {
    pub(crate) fn new_keyword(keyword: &str) -> Term {
        Term::Keyword(Cow::Owned(keyword.to_owned()))
    }

    pub(crate) const fn const_keyword(keyword: &'static str) -> Term {
        Term::Keyword(Cow::Borrowed(keyword))
    }

    pub(crate) fn new_iri(iri: &str) -> Term {
        Term::Iri(Cow::Owned(iri.to_owned()))
    }

    pub(crate) const fn const_iri(iri: &'static str) -> Term {
        Term::Iri(Cow::Borrowed(iri))
    }

    pub(crate) fn as_str(&self) -> &str {
        match self {
            Term::Iri(iri) => &iri,
            Term::Keyword(keyword) => &keyword,
        }
    }

    pub(crate) fn is_iri(&self) -> bool {
        matches!(self, Term::Iri(_))
    }

    pub(crate) fn is_keyword(&self) -> bool {
        matches!(self, Term::Keyword(_))
    }

    pub(crate) fn join(&self, term: &str) -> Term {
        match self {
            Term::Iri(iri) => Term::Iri(Cow::Owned(format!("{}{}", iri, term))),
            Term::Keyword(_) => {
                panic!()
            }
        }
    }
}

pub(crate) const CONTEXT: Term = Term::const_keyword("@context");
pub(crate) const ID: Term = Term::const_keyword("@id");
pub(crate) const TYPE: Term = Term::const_keyword("@type");

pub(crate) const ACTIVITY_STREAMS_NS: Term =
    Term::const_iri("https://www.w3.org/ns/activitystreams");

use std::collections::BTreeMap;

use anyhow::{Context as AnyhowContext, Result, bail};
use serde_json::{Value as JsonValue, json};

use crate::json_ld::vocab::{ACTIVITY_STREAMS_NS, CONTEXT};

use super::{Term, vocab};

#[derive(Debug, Clone, Default)]
pub(crate) struct Context {
    pub(crate) language: Option<String>,
    pub(crate) vocab: Option<Term>,
    pub(crate) term_map: BTreeMap<String, Term>,
    pub(crate) inverse_map: BTreeMap<Term, String>,
}

impl Context {
    pub(crate) fn in_vocab(&self, term_atom: Term) -> Result<bool> {
        if self.vocab.is_none() {
            return Ok(false);
        }
        todo!()
    }
    pub(crate) fn insert(&mut self, term: &str, definition: Term) {
        self.term_map.insert(term.to_owned(), definition);
    }
    pub(crate) fn has_term(&self, term: &str) -> bool {
        self.get_term(term).is_some()
    }
    pub(crate) fn get_term(&self, term: &str) -> Option<&Term> {
        self.term_map.get(term)
    }
}

impl TryFrom<&JsonValue> for Context {
    type Error = anyhow::Error;

    /// Convert node context to an active context using the algorithm defined in
    /// https://www.w3.org/TR/json-ld11-api/#algorithm
    ///
    /// Note - processing is implement just enough to create term definitions
    /// from most valid ActivityPub document in the wild. For example, this will
    /// never implement remote resource fetching.
    ///
    /// Arguably we don't need JSON-LD at all. But I would like to be able to
    /// detect terms redefinition to allow proper extension via JSON-LD, with
    /// the added benefit to store indexed term definitions.
    fn try_from(value: &JsonValue) -> Result<Context> {
        let node = value.as_object().context("value should be a JSON object")?;
        // Per https://www.w3.org/TR/activitystreams-core/#jsonld
        // conformant implementation should assume AS-2.0 vocab.
        // TODO fallback context with AS-2.0 verbs
        let context_def = node
            .get(CONTEXT.as_str())
            .context("node should have a JSON-LD @context")?;
        // 4.1.2.4 Normalize context to an array
        let contexts = if context_def.is_string() || context_def.is_object() {
            vec![context_def.to_owned()]
        } else {
            context_def
                .as_array()
                .context("context should either be a string, an object, or an array of them")?
                .to_owned()
        };

        let mut result = Context::default();
        for context in &contexts {
            match context {
                // 4.1.2.5.1 override
                JsonValue::Null => {
                    result = Context::default();
                }
                // 4.1.2.5.2
                JsonValue::String(remote_context) => {
                    if remote_context == ACTIVITY_STREAMS_NS.as_str() {
                        // result.vocab = Some(ACTIVITY_STREAMS_NS);
                        insert_activitystreams_terms(&mut result);
                    } else {
                        bail!("found unsupported namespace {remote_context}");
                    }
                }
                // 4.1.2.5.4
                JsonValue::Object(_) => {
                    process_context_definition(context, &mut result)?;
                }
                // 4.1.2.5.3
                _ => {
                    bail!("invalid local context (not null, string, or map)");
                }
            }
        }

        Ok(result)
    }
}

fn process_context_definition(context: &JsonValue, result: &mut Context) -> Result<()> {
    let mut defined = BTreeMap::new();

    // 4.1.2.5.5
    match context.get("@version") {
        Some(JsonValue::Number(number)) => {
            if number.as_f64().unwrap_or_default() != 1.1 {
                bail!("invalid @version value {number}");
            }
        }
        Some(value) => {
            bail!("invalid @version value {value}");
        }
        None => {}
    }
    // skip @import

    // skip @base

    // 4.1.2.5.8
    match context.get("@vocab") {
        Some(JsonValue::Null) => {
            result.vocab = None;
        }
        Some(JsonValue::String(value)) => {
            result.vocab = Some(iri_expand(result, value, &context, &mut defined)?);
        }
        Some(value) => bail!("invalid vocabulary mapping {value}"),
        None => {}
    }
    // 4.1.2.5.9
    match context.get("@language") {
        Some(JsonValue::Null) => {
            result.vocab = None;
        }
        Some(JsonValue::String(lang)) => {
            result.language = Some(lang.to_owned());
        }
        Some(value) => bail!("invalid default language {value}"),
        None => {}
    }
    // skip @direction

    // skip @propagate

    // 4.1.2.5.13
    for (key, value) in context.as_object().unwrap().iter() {
        if [
            "@base",
            "@direction",
            "@import",
            "@language",
            "@propagate",
            "@protected",
            "@version",
            "@vocab",
        ]
        .contains(&key.as_str())
        {
            continue;
        }
        create_term_definition(result, &context, key, value, &mut defined)?;
    }

    Ok(())
}

fn create_term_definition(
    result: &mut Context,
    context: &JsonValue,
    term: &str,
    value: &JsonValue,
    defined: &mut BTreeMap<String, bool>,
) -> Result<()> {
    // 4.2.2.1
    match defined.get(term) {
        Some(true) => return Ok(()),
        Some(false) => bail!("cyclic IRI mapping found"),
        _ => {}
    }
    // 4.2.2.2
    if term == "" {
        bail!("invalid term definition (empty string)");
    }
    defined.insert(term.to_owned(), false);

    // skip @type rule - use json-ld-1.0 processing mode

    // 4.2.2.5
    if term.starts_with('@') && term.is_ascii() {
        bail!("keyword redefinition error");
    }
    // 4.2.2.6
    let _previous_definition = result.term_map.remove(term);

    let mut simple_term = true;
    let mut value = value.clone();

    match value {
        // 4.2.2.7
        JsonValue::Null => {
            value = json!({ "@id": null });
        }
        // 4.2.2.8
        JsonValue::String(string) => {
            value = json!({ "@id": string });
            simple_term = true;
        }
        // 4.2.2.9
        JsonValue::Object(_) => {
            simple_term = false;
        }
        // 4.2.2.9
        _ => bail!("invalid term definition error"),
    }

    // 4.2.2.11
    let mut _prefix = false;
    let mut definition = Term::default();

    // skipping @protected processing
    // skipping @type processing
    // skipping @reverse processing

    match value.get("@id") {
        // 4.2.2.14.1
        Some(JsonValue::Null) => {}
        Some(JsonValue::String(id)) => {
            // 4.2.2.14.2.2
            if id.starts_with('@') && !value_is_keyword(id) {
                bail!("invalid keyword alias");
            }
            // 4.2.2.14.2.3
            definition = iri_expand(result, id, context, defined)?;
            if definition == vocab::CONTEXT {
                bail!("invalid keyword alias error (@context cannot be aliased)");
            }
            // 4.2.2.14.2.4
            if term.contains(':') || term.contains('/') {
                // 4.2.2.14.2.4.1
                defined.insert(term.to_owned(), true);
                // 4.2.2.14.2.4.2
                if definition != iri_expand(result, term, context, defined)? {
                    bail!("invalid IRI mapping (term mismatch)");
                }
            // 4.2.2.14.2.5
            } else if simple_term {
                for gen_delim in [':', '/', '?', '#', '[', ']', '@'] {
                    if definition.as_str().ends_with(gen_delim) {
                        _prefix = true;
                    }
                }
            }
        }
        // 4.2.2.14.2.1
        Some(_) => bail!("invalid IRI mapping error (entry is not a string)"),
        None => {
            // 4.2.2.15
            if term.contains(':') {
                if !term.contains("://") {
                    let (term_prefix, suffix) = term.split_once(':').unwrap();
                    // 4.2.2.15.1
                    if context.get(term_prefix).is_some() {
                        create_term_definition(result, context, term_prefix, context, defined)?;
                    // 4.2.2.15.2
                    } else if result.has_term(term_prefix) {
                        definition = result.get_term(term_prefix).unwrap().join(suffix);
                    // 4.2.2.15.3
                    } else {
                        definition = Term::new_iri(term);
                    }
                }
            // 4.2.2.16
            } else if term.contains('/') {
                definition = iri_expand(result, term, context, defined)?;
            // 4.2.2.17
            } else if term == "@type" {
                definition = vocab::TYPE;
            // 4.2.2.18
            } else if result.vocab.is_some() {
                definition = result.vocab.as_ref().unwrap().join(term);
            }
        }
    }

    // skip @container
    // skip @index
    // skip @context
    // skip @language
    // skip @direction
    // skip @nest

    // 4.2.2.25
    match value.get("@prefix") {
        Some(JsonValue::Bool(is_prefix)) => {
            _prefix = *is_prefix;
            if _prefix && definition.is_keyword() {
                bail!("invalid term definition (keyword as prefix)");
            }
        }
        Some(_) => {
            bail!("invalid @prefix value")
        }
        _ => {}
    }

    for entry in value.as_object().unwrap().keys() {
        if ![
            "@id",
            "@reverse",
            "@container",
            "@context",
            "@direction",
            "@index",
            "@language",
            "@nest",
            "@prefix",
            "@protected",
            "@type",
        ]
        .contains(&entry.as_str())
        {
            bail!("invalid term definition (unknown keyword)");
        }
    }

    result.insert(term, definition);
    defined.insert(term.to_owned(), true);

    Ok(())
}

fn iri_expand(
    active_context: &mut Context,
    value: &str,
    local_context: &JsonValue,
    defined: &mut BTreeMap<String, bool>,
) -> Result<Term> {
    // 5.2.2.1
    if value_is_keyword(value) {
        return Ok(Term::Keyword(value.to_owned().into()));
    }
    // 5.2.2.3
    if let Some(entry_value) = local_context.get(value) {
        if defined.get(value).is_none() {
            create_term_definition(active_context, local_context, value, entry_value, defined)?;
        }
    }
    match active_context.get_term(value) {
        Some(definition) => {
            // 5.2.2.4
            if definition.is_keyword() {
                return Ok(definition.clone());
            }
            // assume vocab is true?
            // 5.2.2.5
            return Ok(definition.clone());
        }
        _ => {}
    }
    if value.contains(':') {
        // 5.2.2.6.1
        let (prefix, suffix) = value.split_once(':').unwrap();
        if suffix.starts_with("//") {
            // 5.2.2.6.2
            return Ok(Term::new_iri(value));
        }
        // 5.2.2.6.3
        if let Some(prefix_value) = local_context.get(prefix) {
            if !matches!(defined.get(prefix), Some(true)) {
                create_term_definition(
                    active_context,
                    local_context,
                    prefix,
                    prefix_value,
                    defined,
                )?;
            }
        }
        // 5.2.2.6.4 - assume prefix is true
        if active_context.has_term(prefix) {
            return Ok(active_context.get_term(prefix).unwrap().join(suffix));
        } else {
            // 5.2.2.6.5 - assume IRI form
            return Ok(Term::new_iri(value));
        }
    }
    // 5.2.2.7 - assume vocab is true
    if let Some(vocab) = &active_context.vocab {
        return Ok(vocab.join(value));
    }
    // skip document relative

    Ok(Term::new_iri(value))
}

fn insert_activitystreams_terms(result: &mut Context) {
    result.insert("id", vocab::ID);
    result.insert("type", vocab::TYPE);
}

fn value_is_keyword(value: &str) -> bool {
    [
        "@base",
        "@container",
        "@context",
        "@direction",
        "@graph",
        "@id",
        "@import",
        "@include",
        "@index",
        "@json",
        "@language",
        "@list",
        "@nest",
        "@none",
        "@prefix",
        "@propagate",
        "@protected",
        "@reverse",
        "@set",
        "@type",
        "@value",
        "@version",
        "@vocab",
    ]
    .contains(&value)
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use serde_json::json;

    use crate::json_ld::Term;

    use super::Context;

    #[test]
    fn single_remote_context() -> Result<()> {
        let node = json!(
            {
                "@context": "https://www.w3.org/ns/activitystreams"
            }
        );
        let context = Context::try_from(&node)?;
        assert!(context.has_term("id"));
        assert!(context.has_term("type"));
        Ok(())
    }

    #[test]
    fn single_array_remote_context() -> Result<()> {
        let node = json!(
            {
                "@context": ["https://www.w3.org/ns/activitystreams"]
            }
        );
        let context = Context::try_from(&node)?;
        assert!(context.has_term("id"));
        assert!(context.has_term("type"));
        Ok(())
    }

    #[test]
    fn single_invalid_local_context() -> Result<()> {
        let node = json!(
            {
                "@context": [[]]
            }
        );
        let context = Context::try_from(&node);
        assert!(context.is_err());
        Ok(())
    }

    #[test]
    fn local_context_with_version() -> Result<()> {
        let node = json!(
            {
                "@context": {
                    "@version": 1.1
                }
            }
        );
        let _context = Context::try_from(&node)?;
        Ok(())
    }

    #[test]
    fn local_context_with_invalid_version() -> Result<()> {
        let node = json!(
            {
                "@context": {
                    "@version": 1.2
                }
            }
        );
        let context = Context::try_from(&node);
        assert!(context.is_err());
        Ok(())
    }

    #[test]
    fn local_context_with_invalid_version_null() -> Result<()> {
        let node = json!(
            {
                "@context": {
                    "@version": null
                }
            }
        );
        let context = Context::try_from(&node);
        assert!(context.is_err());
        Ok(())
    }

    #[test]
    fn local_context_with_vocab_iri() -> Result<()> {
        let node = json!(
            {
                "@context": {
                    "@vocab": "http://joinmastodon.org/ns#"
                }
            }
        );
        let context = Context::try_from(&node)?;
        assert_eq!(
            context.vocab,
            Some(Term::new_iri("http://joinmastodon.org/ns#"))
        );
        Ok(())
    }

    #[test]
    fn local_context_with_vocab_invalid() -> Result<()> {
        let node = json!(
            {
                "@context": {
                    "@vocab": []
                }
            }
        );
        let context = Context::try_from(&node);
        assert!(context.is_err());
        Ok(())
    }

    #[test]
    fn local_context_with_language() -> Result<()> {
        let node = json!(
            {
                "@context": {
                    "@language": "zh"
                }
            }
        );
        let context = Context::try_from(&node)?;
        assert_eq!(context.language, Some("zh".to_owned()));
        Ok(())
    }

    #[test]
    fn local_context_with_language_invalid() -> Result<()> {
        let node = json!(
            {
                "@context": {
                    "@language": []
                }
            }
        );
        let context = Context::try_from(&node);
        assert!(context.is_err());
        Ok(())
    }

    #[test]
    fn local_context_with_cyclic_iri_mapping() -> Result<()> {
        let node = json!(
            {
                "@context": {
                    "term": "term:suffix"
                }
            }
        );
        let context = Context::try_from(&node);
        assert!(context.is_err());
        Ok(())
    }

    #[test]
    fn local_context_with_empty_term() -> Result<()> {
        let node = json!(
            {
                "@context": {
                    "": "value"
                }
            }
        );
        let context = Context::try_from(&node);
        assert!(context.is_err());
        Ok(())
    }

    #[test]
    fn local_context_with_keyword_override() -> Result<()> {
        let node = json!(
            {
                "@context": {
                    "@type": "value"
                }
            }
        );
        let context = Context::try_from(&node);
        assert!(context.is_err());
        Ok(())
    }

    #[test]
    fn remote_context_and_local_context() -> Result<()> {
        let node = json!(
            {
               "@context": [
                    "https://www.w3.org/ns/activitystreams",
                    { "ostatus": "http://ostatus.org#" }
               ]
            }
        );
        let context = Context::try_from(&node)?;
        assert!(context.has_term("id"));
        assert!(context.has_term("ostatus"));
        Ok(())
    }

    #[test]
    fn example_mastodon_context() -> Result<()> {
        let node = json!(
            {
                "@context": [
                    "https://www.w3.org/ns/activitystreams",
                    {
                      "ostatus": "http://ostatus.org#",
                      "atomUri": "ostatus:atomUri",
                      "inReplyToAtomUri": "ostatus:inReplyToAtomUri",
                      "conversation": "ostatus:conversation",
                      "sensitive": "as:sensitive",
                      "toot": "http://joinmastodon.org/ns#",
                      "votersCount": "toot:votersCount",
                      "Emoji": "toot:Emoji",
                      "focalPoint": {
                        "@container": "@list",
                        "@id": "toot:focalPoint"
                      }
                    }
                ],
            }
        );
        let context = dbg!(Context::try_from(&node)?);
        assert!(context.has_term("id"));
        assert!(context.has_term("type"));
        assert!(context.has_term("ostatus"));
        assert!(context.has_term("atomUri"));
        assert!(context.has_term("inReplyToAtomUri"));
        assert!(context.has_term("conversation"));
        assert!(context.has_term("sensitive"));
        assert!(context.has_term("toot"));
        assert!(context.has_term("votersCount"));
        assert!(context.has_term("Emoji"));
        assert!(context.has_term("focalPoint"));
        Ok(())
    }
}

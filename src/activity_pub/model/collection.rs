use serde_json::{Number, Value, json};

pub(crate) struct OrderedCollection(Value);

impl OrderedCollection {
    pub(crate) fn new() -> OrderedCollection {
        OrderedCollection(json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": "OrderedCollection"
        }))
    }
    pub(crate) fn id(mut self, link: impl Into<String>) -> OrderedCollection {
        self.0
            .as_object_mut()
            .unwrap()
            .insert("id".to_string(), Value::String(link.into()));
        self
    }
    pub(crate) fn part_of(mut self, link: impl Into<String>) -> OrderedCollection {
        self.0
            .as_object_mut()
            .unwrap()
            .insert("partOf".to_string(), Value::String(link.into()));
        self
    }
    pub(crate) fn with_ordered_items<T>(mut self, items: Vec<T>) -> OrderedCollection
    where
        T: Into<Value>,
    {
        let items = items.into_iter().map(|it| it.into()).collect();
        self.0
            .as_object_mut()
            .unwrap()
            .insert("orderedItems".to_string(), Value::Array(items));
        self
    }
    pub(crate) fn total_items(mut self, total: u64) -> OrderedCollection {
        self.0
            .as_object_mut()
            .unwrap()
            .insert("totalItems".to_string(), Value::Number(Number::from(total)));
        self
    }
    pub(crate) fn first(mut self, link: impl Into<String>) -> OrderedCollection {
        self.0
            .as_object_mut()
            .unwrap()
            .insert("first".to_string(), Value::String(link.into()));
        self
    }
    pub(crate) fn last(mut self, link: impl Into<String>) -> OrderedCollection {
        self.0
            .as_object_mut()
            .unwrap()
            .insert("last".to_string(), Value::String(link.into()));
        self
    }
    pub(crate) fn next(mut self, link: impl Into<String>) -> OrderedCollection {
        self.0
            .as_object_mut()
            .unwrap()
            .insert("next".to_string(), Value::String(link.into()));
        self
    }
    pub(crate) fn prev(mut self, link: impl Into<String>) -> OrderedCollection {
        self.0
            .as_object_mut()
            .unwrap()
            .insert("prev".to_string(), Value::String(link.into()));
        self
    }
    pub(crate) fn into_page(mut self) -> OrderedCollectionPage {
        self.0.as_object_mut().unwrap().insert(
            "type".to_string(),
            Value::String("OrderedCollectionPage".to_string()),
        );
        OrderedCollectionPage(self.0)
    }
}

pub(crate) struct OrderedCollectionPage(Value);

impl From<OrderedCollection> for Value {
    fn from(value: OrderedCollection) -> Self {
        value.0
    }
}

impl From<OrderedCollectionPage> for Value {
    fn from(value: OrderedCollectionPage) -> Self {
        value.0
    }
}

use serde_json::{Number, Value, json};

pub(crate) struct Collection(Value);

impl Collection {
    pub(crate) fn new() -> Collection {
        Collection(json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": "Collection"
        }))
    }
    pub(crate) fn id(mut self, link: impl Into<String>) -> Collection {
        self.0
            .as_object_mut()
            .unwrap()
            .insert("id".to_string(), Value::String(link.into()));
        self
    }
    pub(crate) fn with_items<T>(mut self, items: Vec<T>) -> Collection
    where
        T: Into<Value>,
    {
        let items = items.into_iter().map(|it| it.into()).collect();
        self.0
            .as_object_mut()
            .unwrap()
            .insert("items".to_string(), Value::Array(items));
        self
    }
    pub(crate) fn with_ordered_items<T>(mut self, items: Vec<T>) -> Collection
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
    pub(crate) fn total_items(mut self, total: u64) -> Collection {
        self.0
            .as_object_mut()
            .unwrap()
            .insert("totalItems".to_string(), Value::Number(Number::from(total)));
        self
    }
    pub(crate) fn first(mut self, link: impl Into<String>) -> Collection {
        self.0
            .as_object_mut()
            .unwrap()
            .insert("first".to_string(), Value::String(link.into()));
        self
    }
    pub(crate) fn last(mut self, link: impl Into<String>) -> Collection {
        self.0
            .as_object_mut()
            .unwrap()
            .insert("last".to_string(), Value::String(link.into()));
        self
    }
    pub(crate) fn next(mut self, link: impl Into<String>) -> Collection {
        self.0
            .as_object_mut()
            .unwrap()
            .insert("next".to_string(), Value::String(link.into()));
        self
    }
    pub(crate) fn prev(mut self, link: impl Into<String>) -> Collection {
        self.0
            .as_object_mut()
            .unwrap()
            .insert("prev".to_string(), Value::String(link.into()));
        self
    }
    pub(crate) fn ordered(mut self) -> Collection {
        self.0.as_object_mut().unwrap().insert(
            "type".to_string(),
            Value::String("OrderedCollection".to_string()),
        );
        self
    }
    pub(crate) fn to_page(mut self) -> CollectionPage {
        let ty = self.0.get("type").unwrap().as_str().unwrap();
        match ty {
            "Collection" => {
                self.0.as_object_mut().unwrap().insert(
                    "type".to_string(),
                    Value::String("CollectionPage".to_string()),
                );
            }
            "OrderedCollection" => {
                self.0.as_object_mut().unwrap().insert(
                    "type".to_string(),
                    Value::String("OrderedCollectionPage".to_string()),
                );
            }
            _ => {
                panic!("{ty} is not a Collection type");
            }
        }
        CollectionPage(self.0)
    }
}

pub(crate) struct CollectionPage(Value);

impl From<Collection> for Value {
    fn from(value: Collection) -> Self {
        value.0
    }
}

impl From<CollectionPage> for Value {
    fn from(value: CollectionPage) -> Self {
        value.0
    }
}

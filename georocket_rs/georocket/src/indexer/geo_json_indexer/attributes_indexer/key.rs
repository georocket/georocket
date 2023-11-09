use std::fmt::Display;
use std::fmt::Write;

pub struct Key {
    key: String,
    component_offsets: Vec<usize>,
}

impl Key {
    #[must_use]
    pub fn new() -> Self {
        Self {
            key: String::new(),
            component_offsets: Vec::new(),
        }
    }
    pub fn new_with_root(root: impl Into<String>) -> Self {
        Self {
            key: root.into(),
            component_offsets: Vec::new(),
        }
    }
    pub fn push(&mut self, component: impl Display) -> &mut Self {
        self.component_offsets.push(self.key.len());
        if !self.key.is_empty() {
            self.key.push('.');
        }
        write!(self.key, "{}", component).expect("component could not be written to key");
        self
    }
    pub fn pop(&mut self) -> &mut Self {
        if let Some(offset) = self.component_offsets.pop() {
            self.key.truncate(offset);
        } else {
            self.key.clear();
        }
        self
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn is_empty(&self) -> bool {
        self.key.is_empty()
    }
    pub fn clone_inner(&self) -> String {
        self.key.clone()
    }
}

impl AsRef<str> for Key {
    fn as_ref(&self) -> &str {
        self.key()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pop_empty_key() {
        let mut key = Key::new();
        key.pop();
        assert_eq!("", key.key);
    }

    #[test]
    fn test_push() {
        let mut key = Key::new();
        key.push("base");
        assert_eq!(key.key, "base");
        key.push("component");
        assert_eq!(key.key, "base.component");
    }

    #[test]
    fn test_push_component_with_dot() {
        let mut key = Key::new();
        key.push("base");
        assert_eq!(key.key, "base");
        key.push("component.with.dot");
        assert_eq!(key.key, "base.component.with.dot");
    }

    #[test]
    fn test_pop() {
        let mut key = Key::new();
        key.push("base").push("component").pop();
        assert_eq!(key.key, "base");
        key.pop();
        assert_eq!(key.key, "");
    }

    #[test]
    fn test_pop_component_with_dot() {
        let mut key = Key::new();
        key.push("base.component").push("component.with.dot").pop();
        assert_eq!(key.key, "base.component");
        key.pop();
        assert_eq!(key.key, "");
    }
}

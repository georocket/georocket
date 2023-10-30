pub struct Key {
    key: String,
    component_offsets: Vec<usize>,
}

impl Key {
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
    #[must_use]
    pub fn push(mut self, component: &str) -> Self {
        if self.key.is_empty() {
            self.key = component.into();
        } else {
            self.component_offsets.push(self.key.len());
            self.key.push('.');
            self.key.push_str(&component);
        }
        self
    }
    #[must_use]
    pub fn pop(mut self) -> Self {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push() {
        let mut key = Key::new();
        key = key.push("base");
        assert_eq!(key.key, "base");
        key = key.push("component");
        assert_eq!(key.key, "base.component");
    }

    #[test]
    fn test_push_component_with_dot() {
        let mut key = Key::new();
        key = key.push("base");
        assert_eq!(key.key, "base");
        key = key.push("component.with.dot");
        assert_eq!(key.key, "base.component.with.dot");
    }

    #[test]
    fn test_pop() {
        let mut key = Key::new();
        key = key.push("base").push("component").pop();
        assert_eq!(key.key, "base");
        key = key.pop();
        assert_eq!(key.key, "");
    }

    #[test]
    fn test_pop_component_with_dot() {
        let mut key = Key::new();
        key = key.push("base.component").push("component.with.dot").pop();
        assert_eq!(key.key, "base.component");
        key = key.pop();
        assert_eq!(key.key, "");
    }
}

use tantivy::{query::Scorer, DocId, DocSet, Score, TERMINATED};

/// A scorer that can be used in tests
pub struct IterScorer<I>
where
    I: Iterator<Item = DocId> + Send + 'static,
{
    iter: I,
    current: Option<DocId>,
}

impl<I> IterScorer<I>
where
    I: Iterator<Item = DocId> + Send,
{
    pub fn new(mut iter: I) -> Self {
        let current = iter.next();
        Self { iter, current }
    }
}

impl<I> Scorer for IterScorer<I>
where
    I: Iterator<Item = DocId> + Send,
{
    fn score(&mut self) -> Score {
        0.0
    }
}

impl<I> DocSet for IterScorer<I>
where
    I: Iterator<Item = DocId> + Send,
{
    fn advance(&mut self) -> DocId {
        self.current = self.iter.next();
        self.doc()
    }

    fn doc(&self) -> DocId {
        if let Some(d) = self.current {
            d
        } else {
            TERMINATED
        }
    }

    fn size_hint(&self) -> u32 {
        0
    }
}

#[cfg(test)]
mod tests {
    use assertor::{assert_that, EqualityAssertion};
    use tantivy::{DocSet, TERMINATED};

    use super::IterScorer;

    #[test]
    fn empty() {
        let mut s = IterScorer::new(vec![].into_iter());
        assert_that!(s.doc()).is_equal_to(TERMINATED);
        assert_that!(s.advance()).is_equal_to(TERMINATED);
    }

    #[test]
    fn simple() {
        let mut s = IterScorer::new(vec![1, 2, 3].into_iter());
        assert_that!(s.doc()).is_equal_to(1);
        assert_that!(s.advance()).is_equal_to(2);
        assert_that!(s.doc()).is_equal_to(2);
        assert_that!(s.advance()).is_equal_to(3);
        assert_that!(s.doc()).is_equal_to(3);
        assert_that!(s.advance()).is_equal_to(TERMINATED);
        assert_that!(s.doc()).is_equal_to(TERMINATED);
    }
}

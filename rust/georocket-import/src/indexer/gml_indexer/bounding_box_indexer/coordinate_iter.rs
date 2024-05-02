use super::Dimensions;
use std::num::ParseFloatError;
use std::str::SplitWhitespace;
use thiserror::Error;

#[derive(Debug, Error)]
pub(in crate::indexer::gml_indexer) enum ParseCoordinatesError {
    #[error("number of elements in coordinate string does not match specified dimension")]
    NonMatchingDimensions,
    #[error("invalid floating point representation in coordinates")]
    PareFloatError(#[from] ParseFloatError),
}

pub(in crate::indexer::gml_indexer) struct CoordinateIterator<'a> {
    inner: SplitWhitespace<'a>,
    dimensions: Dimensions,
}

impl<'source> CoordinateIterator<'source> {
    pub(in crate::indexer::gml_indexer) fn new(
        coordinates: &'source str,
        dimensions: Dimensions,
    ) -> Self {
        Self {
            inner: coordinates.split_whitespace(),
            dimensions,
        }
    }
}

impl<'a> Iterator for CoordinateIterator<'a> {
    type Item = Result<(f64, f64), ParseCoordinatesError>;

    fn next(&mut self) -> Option<Self::Item> {
        let x = self.inner.next()?;
        let Some(y) = self.inner.next() else {
            return Some(Err(ParseCoordinatesError::NonMatchingDimensions));
        };
        // throw away z dimension, if dimensionality is 3
        if self.dimensions == Dimensions::D3 {
            if self.inner.next().is_none() {
                return Some(Err(ParseCoordinatesError::NonMatchingDimensions));
            }
        }
        let x = match x.parse() {
            Ok(x) => x,
            Err(e) => return Some(Err(ParseCoordinatesError::PareFloatError(e))),
        };
        let y = match y.parse() {
            Ok(y) => y,
            Err(e) => return Some(Err(ParseCoordinatesError::PareFloatError(e))),
        };
        Some(Ok((x, y)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_dimensional_coordinates() {
        let coordinates = "1 2 3 4";
        let control = [(1., 2.), (3., 4.)].into_iter();
        let iter = CoordinateIterator::new(coordinates, Dimensions::D2);
        for (coord, control) in iter.map(|c| c.unwrap()).zip(control) {
            assert_eq!(coord, control)
        }
    }

    #[test]
    fn three_dimensional_coordinates() {
        let coordinates = "1 2 0 3 4 0";
        let control = [(1., 2.), (3., 4.)].into_iter();
        let iter = CoordinateIterator::new(coordinates, Dimensions::D3);
        for (coord, control) in iter.map(|c| c.unwrap()).zip(control) {
            assert_eq!(coord, control)
        }
    }

    #[test]
    fn three_dimensional_wrong_number() {
        let coordinates = "1 2 0 3 4";
        let mut iter = CoordinateIterator::new(coordinates, Dimensions::D3);
        assert!(iter.next().is_some_and(|c| c.is_ok()));
        let error = iter.next().unwrap().unwrap_err();
        assert!(matches!(
            error,
            ParseCoordinatesError::NonMatchingDimensions
        ));
    }

    #[test]
    fn two_dimensional_wrong_number() {
        let coordinates = "1 2 3 4 0";
        let mut iter = CoordinateIterator::new(coordinates, Dimensions::D2);
        assert!(iter.next().is_some_and(|c| c.is_ok()));
        assert!(iter.next().is_some_and(|c| c.is_ok()));
        let error = iter.next().unwrap().unwrap_err();
        assert!(matches!(
            error,
            ParseCoordinatesError::NonMatchingDimensions
        ));
    }

    #[test]
    fn empty_coordinate_string() {
        let coordinates = "";
        let mut iter = CoordinateIterator::new(coordinates, Dimensions::D2);
        assert!(iter.next().is_none());
        let mut iter = CoordinateIterator::new(coordinates, Dimensions::D3);
        assert!(iter.next().is_none());
    }

    #[test]
    fn wonky_formatting() {
        let coordinates = "  \n1\t2   3 \n\t4";
        let control = [(1., 2.), (3., 4.)].into_iter();
        let iter = CoordinateIterator::new(coordinates, Dimensions::D2);
        for (coord, control) in iter.map(|c| c.unwrap()).zip(control) {
            assert_eq!(coord, control)
        }
    }

    #[test]
    fn malformed_floats() {
        let coordinates = "not valid floats";
        let mut iter = CoordinateIterator::new(coordinates, Dimensions::D3);
        let error = iter.next().unwrap().unwrap_err();
        assert!(matches!(error, ParseCoordinatesError::PareFloatError(_)));
    }
}

use geo::Rect;
use lalrpop_util::lalrpop_mod;
use std::fmt::{Display, Formatter};

use crate::index::Value;

pub mod error;
mod unescape;
lalrpop_mod!(query_parser, "/query/query.rs");

pub use query_parser::QueryParser;

/// Specifies the logical combinator to be to combine the list of `QueryPart`s.
#[derive(Debug, PartialEq)]
pub enum Logical {
    Or(Vec<QueryPart>),
    And(Vec<QueryPart>),
    Not(Box<QueryPart>),
}

/// Specifies how two key-value pairs should be compared to each other
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Operator {
    /// The values must equal
    Eq,

    /// The value of this key-value pair must be greater than the other one
    Gt,

    /// The value of this key-value pair must be greater than or equal to the
    /// other one
    Gte,

    /// The value of this key-value pair must be less than the other one
    Lt,

    /// The value of this key-value pair must be less than or equal to the
    /// other one
    Lte,
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Operator::Eq => write!(f, "="),
            Operator::Lt => write!(f, "<"),
            Operator::Gt => write!(f, ">"),
            Operator::Lte => write!(f, "<="),
            Operator::Gte => write!(f, ">="),
        }
    }
}

/// The top level parts of a `Query`
#[derive(Debug, PartialEq)]
pub enum QueryPart {
    Value(Value),
    BoundingBox(Rect),
    Logical(Logical),
    Comparison {
        operator: Operator,
        key: String,
        value: Value,
    },
}

impl<V> From<V> for QueryPart
where
    V: Into<Value>,
{
    fn from(value: V) -> Self {
        QueryPart::Value(value.into())
    }
}

impl From<Logical> for QueryPart {
    fn from(logic: Logical) -> Self {
        QueryPart::Logical(logic)
    }
}

#[derive(Debug, PartialEq)]
pub enum Query {
    Empty,
    Full(QueryPart),
}

impl From<QueryPart> for Query {
    fn from(part: QueryPart) -> Self {
        Query::Full(part)
    }
}

#[cfg(test)]
macro_rules! query {
    () => {
        $crate::query::Query::Empty
    };
    ($x:expr) => {
        $crate::query::QueryPart::from($x).into()
    };
}

macro_rules! and {
    ($($x:expr),* $(,)?) => {
        $crate::query::QueryPart::Logical(
            $crate::query::Logical::And(vec![$($x.into(),)*]))
    };
}

macro_rules! or {
    ($($x:expr),* $(,)?) => {
        $crate::query::QueryPart::Logical(
            $crate::query::Logical::Or(vec![$($x.into(),)*]))
    };
}

macro_rules! not {
    ($x:expr) => {
        $crate::query::QueryPart::Logical($crate::query::Logical::Not(Box::new($x.into())))
    };
}

macro_rules! eq {
    ($key:expr, $value:expr) => {{
        let key = $key.into();
        let value = $value.into();
        $crate::query::QueryPart::Comparison {
            operator: $crate::query::Operator::Eq,
            key,
            value,
        }
    }};
}

macro_rules! gt {
    ($key:expr, $value:expr) => {{
        let key = $key.into();
        let value = $value.into();
        $crate::query::QueryPart::Comparison {
            operator: $crate::query::Operator::Gt,
            key,
            value,
        }
    }};
}

macro_rules! gte {
    ($key:expr, $value:expr) => {{
        let key = $key.into();
        let value = $value.into();
        $crate::query::QueryPart::Comparison {
            operator: $crate::query::Operator::Gte,
            key,
            value,
        }
    }};
}

macro_rules! lt {
    ($key:expr, $value:expr) => {{
        let key = $key.into();
        let value = $value.into();
        $crate::query::QueryPart::Comparison {
            operator: $crate::query::Operator::Lt,
            key,
            value,
        }
    }};
}

macro_rules! lte {
    ($key:expr, $value:expr) => {{
        let key = $key.into();
        let value = $value.into();
        $crate::query::QueryPart::Comparison {
            operator: $crate::query::Operator::Lte,
            key,
            value,
        }
    }};
}

macro_rules! bbox {
    ($min_x:expr, $min_y:expr, $max_x:expr, $max_y:expr) => {{
        let bbox = geo::Rect::new(
            geo::coord! { x: $min_x, y: $min_y },
            geo::coord! { x: $max_x, y: $max_y },
        );
        $crate::query::QueryPart::BoundingBox(bbox)
    }};
}

pub(crate) use and;
pub(crate) use bbox;
pub(crate) use eq;
pub(crate) use gt;
pub(crate) use gte;
pub(crate) use lt;
pub(crate) use lte;
pub(crate) use not;
pub(crate) use or;
#[cfg(test)]
pub(crate) use query;

#[cfg(test)]
mod tests {
    use lalrpop_util::lexer::Token;
    use lalrpop_util::ParseError;

    use crate::query::error::QueryParserError;

    use super::QueryParser;
    use super::{and, eq, gt, gte, lt, lte, not, or, query};

    #[test]
    fn string() {
        let expected = query!["foo"];
        let qp = QueryParser::new();
        assert_eq!(qp.parse("\"foo\"").unwrap(), expected);
        assert_eq!(qp.parse("\"foo\"  ").unwrap(), expected);
        assert_eq!(qp.parse("  \"foo\"").unwrap(), expected);
        assert_eq!(qp.parse("   \"foo\"  ").unwrap(), expected);
    }

    #[test]
    fn string_with_space() {
        let expected = query!["foo bar"];
        let qp = QueryParser::new();
        assert_eq!(qp.parse("\"foo bar\"").unwrap(), expected);
        assert_eq!(qp.parse("\"foo bar\"  ").unwrap(), expected);
        assert_eq!(qp.parse("  \"foo bar\"").unwrap(), expected);
        assert_eq!(qp.parse("   \"foo bar\"  ").unwrap(), expected);
    }

    #[test]
    fn string_with_escaped_quote() {
        let qp = QueryParser::new();
        assert_eq!(qp.parse("\"foo \\\" bar\"").unwrap(), query!["foo \" bar"]);
    }

    #[test]
    fn string_with_escapes() {
        let qp = QueryParser::new();
        assert_eq!(
            qp.parse("\"foo \\n \\r bar\"").unwrap(),
            query!["foo \n \r bar"]
        );
    }

    #[test]
    fn string_with_escape_unicode() {
        let qp = QueryParser::new();
        assert_eq!(
            qp.parse("\"foo \\u1234 bar\"").unwrap(),
            query!["foo \u{1234} bar"]
        );
    }

    #[test]
    fn string_with_invalid_escape() {
        let qp = QueryParser::new();
        assert_eq!(
            qp.parse(" \"foo \\u123 bar\"  "),
            Err(ParseError::User {
                error: QueryParserError::InvalidHexInUnicodeEscape { location: 11 }
            })
        );
        assert_eq!(
            qp.parse("\"foo \\o bar\""),
            Err(ParseError::User {
                error: QueryParserError::UsupportedEscapeSequence { location: 6 }
            })
        );
        assert_eq!(
            qp.parse("   \"foo \\' bar\""),
            Err(ParseError::User {
                error: QueryParserError::UsupportedEscapeSequence { location: 9 }
            })
        );
    }

    #[test]
    fn numbers() {
        let qp = QueryParser::new();
        assert_eq!(qp.parse("13").unwrap(), query![13]);
        assert_eq!(qp.parse("1.").unwrap(), query![1.]);
        assert_eq!(qp.parse("-1").unwrap(), query![-1]);
        assert_eq!(qp.parse("1.5").unwrap(), query![1.5]);
        assert_eq!(qp.parse("-2.43").unwrap(), query![-2.43]);
        assert_eq!(qp.parse(".05").unwrap(), query![0.05]);
        assert_eq!(qp.parse("1e5").unwrap(), query![1e5]);
        assert_eq!(qp.parse("2e-2").unwrap(), query![2e-2]);
        assert_eq!(qp.parse("10.4e10").unwrap(), query![10.4e10]);
        assert_eq!(qp.parse("10.3E-5").unwrap(), query![10.3E-5]);
        assert_eq!(qp.parse(".4e4").unwrap(), query![0.4e4]);
        assert_eq!(qp.parse(".4E-4").unwrap(), query![0.4E-4]);
        assert_eq!(qp.parse("2.E-5").unwrap(), query![2.0E-5]);
    }

    #[test]
    fn and_strings() {
        let expected = query![and!["foo", "bar"]];
        let qp = QueryParser::new();
        assert_eq!(qp.parse("\"foo\" && \"bar\"").unwrap(), expected);
        assert_eq!(qp.parse("\"foo\" and \"bar\"").unwrap(), expected);
        assert_eq!(qp.parse("\"foo\" AND \"bar\"").unwrap(), expected);
        assert_eq!(qp.parse("\"foo\" aNd \"bar\"").unwrap(), expected);
        assert_eq!(qp.parse("\"foo\" AnD \"bar\"").unwrap(), expected);
        assert_eq!(qp.parse("\"foo\"&&\"bar\"").unwrap(), expected);
    }

    #[test]
    fn and_string_number() {
        let expected = query![and!["foo", 5]];
        let qp = QueryParser::new();
        assert_eq!(qp.parse("\"foo\" && 5").unwrap(), expected);
        assert_eq!(qp.parse("\"foo\" and 5").unwrap(), expected);
        assert_eq!(qp.parse("\"foo\" AND 5").unwrap(), expected);
        assert_eq!(qp.parse("\"foo\" aNd 5").unwrap(), expected);
        assert_eq!(qp.parse("\"foo\" AnD 5").unwrap(), expected);
        assert_eq!(qp.parse("\"foo\"&&5").unwrap(), expected);
    }

    #[test]
    fn and_without_whitespace() {
        let qp = QueryParser::new();
        assert!(matches!(
            qp.parse("\"foo\"AND5"),
            Err(ParseError::UnrecognizedToken {
                token: (5, Token(_, "AND5"), 9),
                ..
            })
        ));
        assert!(matches!(
            qp.parse("\"foo\" AND5"),
            Err(ParseError::UnrecognizedToken {
                token: (6, Token(_, "AND5"), 10),
                ..
            })
        ));
        assert!(matches!(
            qp.parse("\"foo\"AND 5"),
            Err(ParseError::UnrecognizedToken {
                token: (5, Token(_, "AND"), 8),
                ..
            })
        ));
    }

    #[test]
    fn and_multiple() {
        let qp = QueryParser::new();
        assert_eq!(
            qp.parse("13 && \"Hello\" && \"world\"").unwrap(),
            query![and![13, "Hello", "world"]]
        );
    }

    #[test]
    fn or() {
        let expected = query![or!["foo", "bar"]];
        let qp = QueryParser::new();
        assert_eq!(qp.parse("\"foo\" || \"bar\"").unwrap(), expected);
        assert_eq!(qp.parse("\"foo\" or \"bar\"").unwrap(), expected);
        assert_eq!(qp.parse("\"foo\" OR \"bar\"").unwrap(), expected);
        assert_eq!(qp.parse("\"foo\" Or \"bar\"").unwrap(), expected);
        assert_eq!(qp.parse("\"foo\" oR \"bar\"").unwrap(), expected);
        assert_eq!(qp.parse("\"foo\"||\"bar\"").unwrap(), expected);
    }

    #[test]
    fn or_without_whitespace() {
        let qp = QueryParser::new();
        assert!(matches!(
            qp.parse("\"foo\"OR5"),
            Err(ParseError::UnrecognizedToken {
                token: (5, Token(_, "OR5"), 8),
                ..
            })
        ));
        assert!(matches!(
            qp.parse("\"foo\" OR5"),
            Err(ParseError::UnrecognizedToken {
                token: (6, Token(_, "OR5"), 9),
                ..
            })
        ));
        assert!(matches!(
            qp.parse("\"foo\"OR 5"),
            Err(ParseError::UnrecognizedToken {
                token: (5, Token(_, "OR"), 7),
                ..
            })
        ));
    }

    #[test]
    fn or_numbers() {
        let qp = QueryParser::new();
        assert_eq!(
            qp.parse(
                "13 || 1. || -1 || 20 || 1.5 || -2.43 || .05 || 1e5 ||
                2e-2 || 10.4e10 || -10.3E-5 || .4e4 || .4E-4 || 2.E-5"
            )
            .unwrap(),
            query![or![
                13, 1., -1, 20, 1.5, -2.43, 0.05, 1e5, 2e-2, 10.4e10, -10.3E-5, 0.4e4, 0.4E-4,
                2.0E-5,
            ]]
        );
    }

    #[test]
    fn quoted_or() {
        let qp = QueryParser::new();
        assert_eq!(
            qp.parse("\"\\\"foo\\\" || \\\"bar\\\"\"").unwrap(),
            query!["\"foo\" || \"bar\""]
        );
    }

    #[test]
    fn or_multiple() {
        let qp = QueryParser::new();
        assert_eq!(
            qp.parse("13 || \"Hello\" || \"world\"").unwrap(),
            query![or![13, "Hello", "world"]]
        );
    }

    #[test]
    fn logical_operator_precedence() {
        let qp = QueryParser::new();
        assert_eq!(
            qp.parse("\"foo\" && \"bar\" || \"baz\"").unwrap(),
            query![or![and!["foo", "bar"], "baz"]]
        );
        assert_eq!(
            qp.parse("(\"foo\" && \"bar\") || \"baz\"").unwrap(),
            query![or![and!["foo", "bar"], "baz"]]
        );
        assert_eq!(
            qp.parse("\"foo\" && (\"bar\" || \"baz\")").unwrap(),
            query![and!["foo", or!["bar", "baz"]]]
        );
    }

    #[test]
    fn not() {
        let qp = QueryParser::new();
        assert_eq!(qp.parse("!(\"foo\")").unwrap(), query![not!["foo"]]);
        assert_eq!(qp.parse(" !  (\"foo\")").unwrap(), query![not!["foo"]]);
        assert_eq!(
            qp.parse("!(\"foo\" || \"bar\")").unwrap(),
            query![not![or!["foo", "bar"]]]
        );
    }

    #[test]
    fn identifier() {
        let qp = QueryParser::new();
        assert_eq!(
            qp.parse("foo == \"bar\"").unwrap(),
            query![eq!["foo", "bar"]]
        );
        assert_eq!(
            qp.parse("_fo_o == \"bar\"").unwrap(),
            query![eq!["_fo_o", "bar"]]
        );
        assert_eq!(
            qp.parse("foo500 == \"bar\"").unwrap(),
            query![eq!["foo500", "bar"]]
        );
        assert_eq!(
            qp.parse("f5o5o5 == \"bar\"").unwrap(),
            query![eq!["f5o5o5", "bar"]]
        );
        assert_eq!(
            qp.parse("fooäöü == \"bar\"").unwrap(),
            query![eq!["fooäöü", "bar"]]
        );
    }

    #[test]
    fn eq() {
        let expected = query![eq!["foo", "bar"]];
        let qp = QueryParser::new();
        assert_eq!(qp.parse("foo == \"bar\"").unwrap(), expected);
        assert_eq!(qp.parse("foo = \"bar\"").unwrap(), expected);
        assert_eq!(qp.parse("foo==\"bar\"").unwrap(), expected);
        assert_eq!(qp.parse("foo=\"bar\"").unwrap(), expected);

        assert_eq!(qp.parse("foo == 5").unwrap(), query![eq!["foo", 5]]);
        assert_eq!(
            qp.parse("`foo bar` == 5").unwrap(),
            query![eq!["foo bar", 5]]
        );
        assert_eq!(
            qp.parse("`foo \\` bar` == 5").unwrap(),
            query![eq!["foo ` bar", 5]]
        );
    }

    #[test]
    fn neq() {
        let expected = query![not![eq!["foo", "bar"]]];
        let qp = QueryParser::new();
        assert_eq!(qp.parse("foo != \"bar\"").unwrap(), expected);
        assert_eq!(qp.parse("foo!=\"bar\"").unwrap(), expected);
        assert_eq!(qp.parse("foo !== \"bar\"").unwrap(), expected);
        assert_eq!(qp.parse("foo!==\"bar\"").unwrap(), expected);
    }

    #[test]
    fn gt() {
        let expected = query![gt!["height", 12]];
        let qp = QueryParser::new();
        assert_eq!(qp.parse("height > 12").unwrap(), expected);
        assert_eq!(qp.parse("height>12").unwrap(), expected);
    }

    #[test]
    fn gte() {
        let expected = query![gte!["height", 12.]];
        let qp = QueryParser::new();
        assert_eq!(qp.parse("height >= 12.").unwrap(), expected);
        assert_eq!(qp.parse("height>=12.").unwrap(), expected);
    }

    #[test]
    fn lt() {
        let expected = query![lt!["height", -12]];
        let qp = QueryParser::new();
        assert_eq!(qp.parse("height < -12").unwrap(), expected);
        assert_eq!(qp.parse("height<-12").unwrap(), expected);
    }

    #[test]
    fn lte() {
        let expected = query![lte!["height", -12e5]];
        let qp = QueryParser::new();
        assert_eq!(qp.parse("height <= -12e5").unwrap(), expected);
        assert_eq!(qp.parse("height<=-12e5").unwrap(), expected);
    }

    #[test]
    fn bbox() {
        let expected = query![bbox![1.0, 2.0, 3.0, 4.0]];
        let qp = QueryParser::new();
        assert_eq!(qp.parse("bbox(1, 2, 3, 4)").unwrap(), expected);
        assert_eq!(qp.parse("BBOX  (  1.0   ,2, 3.0, 4  )").unwrap(), expected);
    }

    #[test]
    fn bbox_error() {
        let qp = QueryParser::new();
        assert!(matches!(
            qp.parse("bbox(1, 2, 3)"),
            Err(ParseError::UnrecognizedToken {
                token: (12, Token(_, ")"), 13),
                ..
            })
        ));
        assert!(matches!(
            qp.parse("bbox(1, 2, 3,)"),
            Err(ParseError::UnrecognizedToken {
                token: (13, Token(_, ")"), 14),
                ..
            })
        ));
    }

    #[test]
    fn complex() {
        let qp = QueryParser::new();
        assert_eq!(
            qp.parse(
                "(\"foo\" && \"bar\" AND (\"test1\" || \"test2\")) ||
                \"hello\" OR !(\"world\" or \"you\") && bbox(0,1,2,3)"
            )
            .unwrap(),
            query![or![
                and!["foo", "bar", or!["test1", "test2"]],
                "hello",
                and![not![or!["world", "you"]], bbox![0.0, 1.0, 2.0, 3.0],]
            ]]
        );
    }
}

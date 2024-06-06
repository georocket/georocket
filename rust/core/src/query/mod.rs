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

#[cfg(test)]
macro_rules! not {
    ($x:expr) => {
        $crate::query::QueryPart::Logical($crate::query::Logical::Not(Box::new($x.into())))
    };
}

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

pub(crate) use and;
#[cfg(test)]
pub(crate) use eq;
#[cfg(test)]
pub(crate) use gt;
#[cfg(test)]
pub(crate) use gte;
#[cfg(test)]
pub(crate) use lt;
#[cfg(test)]
pub(crate) use lte;
#[cfg(test)]
pub(crate) use not;
pub(crate) use or;
#[cfg(test)]
pub(crate) use query;

#[cfg(test)]
mod tests {
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
        assert_eq!(
            qp.parse("\"foo\"AND5"),
            Err(ParseError::InvalidToken { location: 5 })
        );
        assert_eq!(
            qp.parse("\"foo\" AND5"),
            Err(ParseError::InvalidToken { location: 6 })
        );
        assert_eq!(
            qp.parse("\"foo\"AND 5"),
            Err(ParseError::InvalidToken { location: 5 })
        );
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
        assert_eq!(
            qp.parse("\"foo\"OR5"),
            Err(ParseError::InvalidToken { location: 5 })
        );
        assert_eq!(
            qp.parse("\"foo\" OR5"),
            Err(ParseError::InvalidToken { location: 6 })
        );
        assert_eq!(
            qp.parse("\"foo\"OR 5"),
            Err(ParseError::InvalidToken { location: 5 })
        );
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

    // #[test]
    // fn eq() {
    //     assert_eq!(
    //         compile_query("EQ(foo bar)").unwrap(),
    //         query![eq!["foo", "bar"]]
    //     );
    //     assert_eq!(
    //         compile_query("EQ  (  foo bar   )  ").unwrap(),
    //         query![eq!["foo", "bar"]]
    //     );
    //     assert_eq!(
    //         compile_query("EQ(foo \"bar\")").unwrap(),
    //         query![eq!["foo", "bar"]]
    //     );
    //     assert_eq!(
    //         compile_query("EQ(\"foo\" \"bar\")").unwrap(),
    //         query![eq!["foo", "bar"]]
    //     );
    //     assert_eq!(
    //         compile_query("EQ('foo' bar)").unwrap(),
    //         query![eq!["foo", "bar"]]
    //     );
    //     assert_eq!(
    //         compile_query("EQ('foo' 'bar')").unwrap(),
    //         query![eq!["foo", "bar"]]
    //     );
    //     assert_eq!(compile_query("EQ(foo 4)").unwrap(), query![eq!["foo", 4]]);
    // }

    // #[test]
    // fn gt() {
    //     assert_eq!(
    //         compile_query("GT(height 12)").unwrap(),
    //         query![gt!["height", 12]]
    //     );
    // }

    // #[test]
    // fn gte() {
    //     assert_eq!(
    //         compile_query("GTE(height 12.0  )").unwrap(),
    //         query![gte!["height", 12.]]
    //     );
    // }

    // #[test]
    // fn lt() {
    //     assert_eq!(
    //         compile_query("LT(height -12.)").unwrap(),
    //         query![lt!["height", -12.]]
    //     );
    // }

    // #[test]
    // fn lte() {
    //     assert_eq!(
    //         compile_query("LTE(height -12e5)").unwrap(),
    //         query![lte!["height", -12e5]]
    //     );
    // }

    // #[test]
    // fn not_eq() {
    //     assert_eq!(
    //         compile_query("NOT(EQ(foo bar))").unwrap(),
    //         query![not![eq!["foo", "bar"]]]
    //     );
    // }

    // #[test]
    // fn complex() {
    //     assert_eq!(
    //         compile_query("AND(foo bar OR(test1 test2)) hello NOT(world \"you\")").unwrap(),
    //         query![
    //             and!["foo", "bar", or!["test1", "test2"]],
    //             "hello",
    //             not!["world", "you"]
    //         ]
    //     );
    // }
}

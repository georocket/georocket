use anyhow::Result;
use georocket_core::query::error::QueryParserError;
use lalrpop_util::ParseError;
use thiserror::Error;
use yansi::{Condition, Paint};

/// An error that happened during search
#[derive(Error, Debug)]
pub enum SearchError {
    #[error("{0}")]
    Parse(String),
}

/// Try to convert a [ParseError] into a [SearchError]
pub trait TryIntoSearchError {
    fn try_into_search_error(self, query: &str) -> Result<SearchError>;
}

fn expected_to_string(expected: &[String]) -> String {
    let mut result = String::new();
    for (i, e) in expected.iter().enumerate() {
        let sep = match i {
            0 => {
                if expected.len() > 1 {
                    " Expected one of"
                } else {
                    " Expected"
                }
            }
            _ if i < expected.len() - 1 => ",",
            _ => ", or",
        };
        result.push_str(sep);
        result.push(' ');
        result.push_str(e);
    }
    if !result.is_empty() {
        result.push('.');
    }
    result
}

impl<T> TryIntoSearchError for ParseError<usize, T, QueryParserError> {
    fn try_into_search_error(self, query: &str) -> Result<SearchError> {
        let (msg, span) = match self {
            ParseError::InvalidToken { location } => {
                ("Invalid token".to_string(), location..location)
            }
            ParseError::UnrecognizedEof { location, expected } => {
                if query.trim().is_empty() {
                    return Ok(SearchError::Parse("Query must not be empty".to_string()));
                } else {
                    (
                        format!("Unrecognized EOF.{}", expected_to_string(&expected)),
                        location..location,
                    )
                }
            }
            ParseError::UnrecognizedToken { token, expected } => (
                format!("Unrecognized token.{}", expected_to_string(&expected)),
                token.0..token.2,
            ),
            ParseError::ExtraToken { token } => ("Extra token.".to_string(), token.0..token.2),
            ParseError::User { error } => match error {
                QueryParserError::InvalidNumber { start, end } => {
                    ("Invalid number.".to_string(), start..end)
                }
                QueryParserError::ExpectedEscapeCharacter { location } => (
                    "Expected character after backslash.".to_string(),
                    location..location + 1,
                ),
                QueryParserError::UsupportedEscapeSequence { location } => (
                    "Unsupported escape sequence (unexpected character after backslash)."
                        .to_string(),
                    location..location + 1,
                ),
                QueryParserError::ExpectedHexInUnicodeEscape { location } => (
                    "Expected hex character in unicode escape sequence.".to_string(),
                    location..location + 1,
                ),
                QueryParserError::InvalidHexInUnicodeEscape { location } => (
                    "Found invalid hex character in unicode escape sequence.".to_string(),
                    location..location + 1,
                ),
                QueryParserError::InvalidUnicodeEscapeSequence { start, end } => (
                    "Unicode escape sequence could not be converted to a character.".to_string(),
                    start..end,
                ),
            },
        };

        // extract snippet
        let bytes = query.as_bytes();
        let mut snippet_start = span.start;
        while snippet_start > 0 && bytes[snippet_start - 1] != b'\n' {
            snippet_start -= 1;
        }
        let mut snippet_end = span.end;
        while snippet_end < bytes.len() && bytes[snippet_end] != b'\n' {
            snippet_end += 1;
        }
        let snippet = &query[snippet_start..snippet_end];

        // format message
        let span_len = span.end - span.start;
        let prefix = span.start - snippet_start;
        let center_prefix = ((span_len + 1) / 2).saturating_sub(1);
        let center_suffix = (span_len) / 2;
        Ok(SearchError::Parse(
            format!(
                "Unable to parse query\n\n{}{}{}\n{}{}{}{}\n{}{}{}",
                &snippet[0..prefix],
                &snippet[span.start..span.end].red(),
                &snippet[span.end..],
                " ".repeat(prefix),
                "─".repeat(center_prefix).red(),
                (if span.end > span.start { "┬" } else { "│" }).red(),
                "─".repeat(center_suffix).red(),
                " ".repeat(prefix + center_prefix),
                "╰── ".red(),
                msg.red().bold()
            )
            .whenever(Condition::from(|| {
                Condition::stderr_is_tty() && Condition::clicolor() && Condition::no_color()
            }))
            .to_string(),
        ))
    }
}

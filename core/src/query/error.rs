/// Custom errors from the QueryParser
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum QueryParserError {
    /// Number could not be parsed
    InvalidNumber { start: usize, end: usize },

    /// Expected character after backslash
    ExpectedEscapeCharacter { location: usize },

    /// Unsupported escape sequence (unexpected character after backslash)
    UsupportedEscapeSequence { location: usize },

    /// Expected hex character in unicode escape sequence
    ExpectedHexInUnicodeEscape { location: usize },

    /// Found invalid hex character in unicode escape sequence
    InvalidHexInUnicodeEscape { location: usize },

    /// Unicode escape sequence could not be converted to a character
    InvalidUnicodeEscapeSequence { start: usize, end: usize },
}

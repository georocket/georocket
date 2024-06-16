use std::borrow::Cow;

use lalrpop_util::{lexer::Token, ParseError};

use super::error::QueryParserError;

/// Unescapes any JSON escape characters in the given string (see [https://www.json.org]).
/// Returns the original string if it does not contain escape characters.
fn unescape(
    s: &str,
    quote_character: char,
    pos: usize,
) -> Result<Cow<str>, ParseError<usize, Token, QueryParserError>> {
    use ParseError::User;
    use QueryParserError::*;

    if !s.contains('\\') {
        return Ok(Cow::from(s));
    }

    let mut result = String::new();
    let mut chars = s.char_indices();
    while let Some((i, c)) = chars.next() {
        let p = if c == '\\' {
            // get escaped character
            let (j, n) = chars.next().ok_or(User {
                error: ExpectedEscapeCharacter { location: pos + i },
            })?;

            match n {
                _ if n == quote_character => n,
                '\\' | '/' => n,
                'b' => '\x08',
                'f' => '\x0C',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',

                'u' => {
                    // unicode escape ...
                    // get next 4 hex characters and convert them to u32
                    let mut u = 0u32;
                    for k in 0..4 {
                        let (xi, x) = chars.next().ok_or(User {
                            error: ExpectedHexInUnicodeEscape {
                                location: pos + j + k + 1,
                            },
                        })?;

                        let xu = x.to_digit(16).ok_or(User {
                            error: InvalidHexInUnicodeEscape { location: pos + xi },
                        })?;

                        u <<= 4;
                        u |= xu;
                    }

                    char::from_u32(u).ok_or(User {
                        error: InvalidUnicodeEscapeSequence {
                            start: pos + j,
                            end: pos + j + 4,
                        },
                    })?
                }

                _ => {
                    return Err(User {
                        error: UsupportedEscapeSequence { location: pos + j },
                    })
                }
            }
        } else {
            c
        };

        result.push(p);
    }

    Ok(Cow::from(result))
}

/// Unescapes any JSON escape characters (see [https://www.json.org]) in the
/// given double-quoted string. Returns the original string if it does not
/// contain escape characters.
pub fn unescape_string(
    s: &str,
    pos: usize,
) -> Result<Cow<str>, ParseError<usize, Token, QueryParserError>> {
    unescape(s, '"', pos)
}

/// Unescapes any JSON escape characters (see [https://www.json.org]) in the
/// given quoted identifier. Returns the original string if it does not contain
/// escape characters.
pub fn unescape_identifier(
    s: &str,
    pos: usize,
) -> Result<Cow<str>, ParseError<usize, Token, QueryParserError>> {
    unescape(s, '`', pos)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use lalrpop_util::ParseError::User;

    use crate::query::error::QueryParserError::*;

    use super::{unescape_identifier, unescape_string};

    #[test]
    fn no_escape() {
        assert_eq!(
            unescape_string("Hello world", 0),
            Ok(Cow::from("Hello world"))
        );
    }

    #[test]
    fn single_escape() {
        assert_eq!(unescape_string("\\n", 0), Ok(Cow::from("\n")));
    }

    #[test]
    fn simple_escapes() {
        assert_eq!(
            unescape_string(
                "\\\"Hello \\b world \\f ! This \\n is \\r an \\t escaped \\\\ string \\/ .",
                0
            ),
            Ok(Cow::from(
                "\"Hello \x08 world \x0C ! This \n is \r an \t escaped \\ string / ."
            )),
        );
    }

    #[test]
    fn unicode() {
        assert_eq!(
            unescape_string("Hello \\uA1C4 world.\\n", 0),
            Ok(Cow::from("Hello \u{A1C4} world.\n"))
        );
    }

    #[test]
    fn invalid_unicode() {
        assert_eq!(
            unescape_string("Hello   \\u", 10),
            Err(User {
                error: ExpectedHexInUnicodeEscape { location: 20 }
            })
        );

        assert_eq!(
            unescape_string("Hello \\u world.", 0),
            Err(User {
                error: InvalidHexInUnicodeEscape { location: 8 }
            })
        );

        assert_eq!(
            unescape_string("Hello \\uz world.", 1),
            Err(User {
                error: InvalidHexInUnicodeEscape { location: 9 }
            })
        );

        assert_eq!(
            unescape_string("Hello \\uabz world.", 5),
            Err(User {
                error: InvalidHexInUnicodeEscape { location: 15 }
            })
        );
    }

    #[test]
    fn invalid_escape_character() {
        assert_eq!(
            unescape_string("Hello \\i world.", 5),
            Err(User {
                error: UsupportedEscapeSequence { location: 12 }
            })
        );
    }

    #[test]
    fn identifier() {
        assert_eq!(unescape_identifier("and", 0), Ok(Cow::from("and")));
        assert_eq!(unescape_identifier("an\\`d", 0), Ok(Cow::from("an`d")));
    }
}

use std::borrow::Cow;

use lalrpop_util::{lexer::Token, ParseError};

use super::error::QueryParserError;

/// Unescapes any JSON escape characters in the given string (see [https://www.json.org]).
/// Returns the original string if it does not contain escape characters.
pub fn unescape(
    s: &str,
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
            let Some((j, n)) = chars.next() else {
                return Err(User {
                    error: ExpectedEscapeCharacter { location: pos + i },
                });
            };

            match n {
                '"' | '\\' | '/' => n,
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
                        let Some((xi, x)) = chars.next() else {
                            return Err(User {
                                error: ExpectedHexInUnicodeEscape {
                                    location: pos + j + k + 1,
                                },
                            });
                        };

                        let Some(xu) = x.to_digit(16) else {
                            return Err(User {
                                error: InvalidHexInUnicodeEscape { location: pos + xi },
                            });
                        };

                        u <<= 4;
                        u |= xu;
                    }

                    match char::from_u32(u) {
                        Some(d) => d,
                        None => {
                            return Err(User {
                                error: InvalidUnicodeEscapeSequence {
                                    start: pos + j,
                                    end: pos + j + 4,
                                },
                            })
                        }
                    }
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

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use lalrpop_util::ParseError::User;

    use crate::query::error::QueryParserError::*;

    use super::unescape;

    #[test]
    fn no_escape() {
        assert_eq!(unescape("Hello world", 0), Ok(Cow::from("Hello world")));
    }

    #[test]
    fn single_escape() {
        assert_eq!(unescape("\\n", 0), Ok(Cow::from("\n")));
    }

    #[test]
    fn simple_escapes() {
        assert_eq!(
            unescape(
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
            unescape("Hello \\uA1C4 world.\\n", 0),
            Ok(Cow::from("Hello \u{A1C4} world.\n"))
        );
    }

    #[test]
    fn invalid_unicode() {
        assert_eq!(
            unescape("Hello   \\u", 10),
            Err(User {
                error: ExpectedHexInUnicodeEscape { location: 20 }
            })
        );

        assert_eq!(
            unescape("Hello \\u world.", 0),
            Err(User {
                error: InvalidHexInUnicodeEscape { location: 8 }
            })
        );

        assert_eq!(
            unescape("Hello \\uz world.", 1),
            Err(User {
                error: InvalidHexInUnicodeEscape { location: 9 }
            })
        );

        assert_eq!(
            unescape("Hello \\uabz world.", 5),
            Err(User {
                error: InvalidHexInUnicodeEscape { location: 15 }
            })
        );
    }

    #[test]
    fn invalid_escape_character() {
        assert_eq!(
            unescape("Hello \\i world.", 5),
            Err(User {
                error: UsupportedEscapeSequence { location: 12 }
            })
        );
    }
}

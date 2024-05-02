use regex::Regex;
use std::num::ParseIntError;
use std::ops::RangeInclusive;
use thiserror::Error;

/// The errors which can occur when attempting to retrieve the EPSG code from a supplied string.
#[derive(Debug, Error)]
pub(in crate::indexer::gml_indexer) enum ParseEPSGError {
    /// Indicates that the patters (see [`EPSG_CODE_REGEX`]) was not found in the supplied string.
    #[error("the pattern was not found in the supplied string: {0}")]
    PatternNotFound(String),
    /// Indicates that the code in the supplied string falls outside the range of valid EPSG codes.
    #[error(
        "invalid EPSG code in supplied string: {0}. EPSG codes must be in the range [1024, 32766]"
    )]
    CodeOutOfRange(String),
    /// Indicates that there was an issue with parsing the code captured from the supplied string.
    #[error("an error occurred parsing the epsg code in the supplied string: {1}")]
    InternalParseError(ParseIntError, String),
}

/// The regex used to search for EPSG codes.
/// # Breakdown
/// 1. "EPSG:": The pattern begins with the string "EPSG:"
/// 2. "(?:\d+(?:\.\d+)?:)?": The pattern optionally contains a version number in the form of <MAJOR>.<MINOR>,
///     where the minor number is optional.
///     2.1 "\d+":
/// 3. "(\d+)": Capture group for the last part of the pattern, which is the EPSG code. Captures all digits, regardless
///     of the amount. Codes outside the valid range are manually discarded with appropriate errors.
const EPSG_CODE_REGEX: &str = r"EPSG:(?:\d+(?:\.\d+)?:)?(\d+)";
/// EPSG codes are numbers within the range [1024, 32766]
///
/// "Codes for primary entity types are within the range 1024 to 32766 inclusive." - Policies and procedures for EPSG Dataset data management, section 6.2
const EPSG_CODE_RANGE: RangeInclusive<u32> = 1024..=32766;

#[inline(always)]
fn get_epsg_code_regex() -> Regex {
    /// # Panic Safety
    /// We have asserted in a unit test, that the regex supplied to `Regex::new` is valid. If unwrapping would
    /// lead to an error, the unit test `epsg_code_regex_is_valid` would have failed.
    Regex::new(EPSG_CODE_REGEX).expect("the supplied regex should be valid")
}

/// Retrieve the first EPSG code found in the provided `srs_name` string.
///
/// # Errors
/// See [`ParseEPSGError`] for details.
pub(in crate::indexer::gml_indexer) fn get_epsg_code(
    srs_name: &str,
) -> Result<u16, ParseEPSGError> {
    use self::ParseEPSGError::{CodeOutOfRange, InternalParseError, PatternNotFound};
    let regex = get_epsg_code_regex();
    let captures = regex
        .captures(srs_name)
        .ok_or(PatternNotFound(srs_name.to_owned()))?;
    /// # Panic Safety:
    /// We have asserted in a unit test, that the capture group is valid for the regex. If unwrapping would lead
    /// to an error, the unit test `epsg_code_regex_capture_group_1_is_valid` would have failed.
    let code = captures
        .get(1)
        .expect("the capture group should be valid")
        .as_str();
    // reject codes with more than five digits
    if code.len() > 5 {
        return Err(CodeOutOfRange(srs_name.to_owned()));
    }
    // parse to an u32, as all numbers with five or fewer decimal digits will always parse to a valid u32, which
    // is not true for u16.
    let code: u32 = code
        .parse()
        .map_err(|err| InternalParseError(err, code.to_owned()))?;
    if EPSG_CODE_RANGE.contains(&code) {
        // we can safely narrow down to 16 bits
        Ok(code as u16)
    } else {
        Err(CodeOutOfRange(srs_name.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// A simple test to assert that our regex (see [`EPSG_CODE_REGEX`]) is valid and can be compiled.
    /// This test is useful, as it enables us to safely unwrap the value returned by `Regex::new(REGEX)`.
    #[test]
    fn epsg_code_regex_is_valid() {
        let regex = Regex::new(EPSG_CODE_REGEX);
        assert!(regex.is_ok());
    }

    /// A simple function that asserts that the capture group 1 of our regex (see [`EPSG_CODE_REGEX`]) is valid.
    /// This allows us to safely unwrap the value returned by `get_epsg_code_regex().captures(srs_name)
    #[test]
    fn epsg_code_regex_capture_group_1_is_valid() {
        let regex = get_epsg_code_regex();
        let captures = regex.captures("EPSG:89223").unwrap();
        assert!(captures.get(1).is_some());
    }

    /// Tests if four and five-digit codes can be extracted from a simple string
    #[test]
    fn test_get_code_simple() {
        let code = get_epsg_code("EPSG:1234").unwrap();
        assert_eq!(code, 1234);
        let code = get_epsg_code("EPSG:12345").unwrap();
        assert_eq!(code, 12345);
    }

    #[test]
    fn test_get_code_version() {
        let code = get_epsg_code("EPSG:1.7:3234").unwrap();
        assert_eq!(code, 3234)
    }
    /// Tests edge cases that are valid codes
    #[test]
    fn test_valid_edge_cases() {
        let code = get_epsg_code("EPSG:1024").unwrap();
        assert_eq!(code, 1024);
        let code = get_epsg_code("EPSG:32766").unwrap();
        assert_eq!(code, 32766);
    }

    /// Tests if codes which are outside the range of valid EPSG codes are rejected with the appropriate error
    #[test]
    fn test_out_of_range() {
        let out_of_range = get_epsg_code("EPSG:42").unwrap_err();
        assert!(matches!(out_of_range, ParseEPSGError::CodeOutOfRange(_)));
        let out_of_range = get_epsg_code("EPSG:01000").unwrap_err();
        assert!(matches!(out_of_range, ParseEPSGError::CodeOutOfRange(_)));
        let out_of_range = get_epsg_code("EPSG:782345").unwrap_err();
        assert!(matches!(out_of_range, ParseEPSGError::CodeOutOfRange(_)));
        let out_of_range = get_epsg_code(
            "EPSG:675987645345324536987867567546736809787565345234253759709809809786456",
        )
        .unwrap_err();
        assert!(matches!(out_of_range, ParseEPSGError::CodeOutOfRange(_)));
    }

    /// Tests edge cases which should be rejected
    #[test]
    fn test_out_of_range_edge_cases() {
        let out_of_range = get_epsg_code("EPSG:1023").unwrap_err();
        assert!(matches!(out_of_range, ParseEPSGError::CodeOutOfRange(_)));
        let out_of_range = get_epsg_code("EPSG:32767").unwrap_err();
        assert!(matches!(out_of_range, ParseEPSGError::CodeOutOfRange(_)));
    }
}

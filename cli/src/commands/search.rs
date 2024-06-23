use std::{thread::spawn, time::Instant};

use crossbeam_channel::bounded;
use georocket_core::{
    index::{tantivy::TantivyIndex, Index},
    query::{error::QueryParserError, QueryParser},
    storage::{rocksdb::RocksDBStore, Store},
};

use anyhow::{bail, Context, Result};
use ariadne::{Color, Label, Report, ReportKind, Source};
use clap::Args;

/// Search the GeoRocket data store
#[derive(Args, Debug)]
pub struct SearchArgs {
    /// The search query
    pub query: String,
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

/// Run the `search` command
pub fn run_search(args: SearchArgs) -> Result<()> {
    // initialize store
    let store = RocksDBStore::new("store")?;

    // initialize index
    let index = TantivyIndex::new("index")?;

    let search_start = Instant::now();
    let mut first = false;
    let mut found_chunks = 0;

    let query_parser = QueryParser::new();
    let compiler_result = query_parser.parse(&args.query);
    match compiler_result {
        Ok(query) => {
            let (search_sender, search_receiver) = bounded(1024 * 10);

            let search_thread = spawn(move || {
                for meta in index.search(query)? {
                    let meta = meta?;
                    search_sender.send(meta)?;
                }
                anyhow::Ok(())
            });

            for meta in search_receiver {
                // TODO remove this
                if !first {
                    println!("Found first chunk after {:?}", search_start.elapsed());
                    first = true;
                }

                let chunk = store
                    .get(meta.id)?
                    .with_context(|| format!("Unable to find chunk with ID `{}'", meta.id))?;

                // TODO implement merger
                println!("{}", std::str::from_utf8(&chunk).unwrap());

                found_chunks += 1;
            }

            if let Err(err) = search_thread.join() {
                bail!("Search thread threw an error: {err:?}");
            }

            // TODO remove this
            println!(
                "Found {} chunks in {:?}",
                found_chunks,
                search_start.elapsed()
            );
        }
        Err(err) => {
            let (msg, span) = match err {
                lalrpop_util::ParseError::InvalidToken { location } => {
                    ("Invalid token".to_string(), location..location)
                }
                lalrpop_util::ParseError::UnrecognizedEof { location, expected } => (
                    format!("Unrecognized EOF.{}", expected_to_string(&expected)),
                    location..location,
                ),
                lalrpop_util::ParseError::UnrecognizedToken { token, expected } => (
                    format!("Unrecognized token.{}", expected_to_string(&expected)),
                    token.0..token.2,
                ),
                lalrpop_util::ParseError::ExtraToken { token } => {
                    ("Extra token.".to_string(), token.0..token.2)
                }
                lalrpop_util::ParseError::User { error } => match error {
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
                        "Unicode escape sequence could not be converted to a character."
                            .to_string(),
                        start..end,
                    ),
                },
            };
            Report::build(ReportKind::Error, "query", span.start)
                .with_label(
                    Label::new(("query", span))
                        .with_message(msg)
                        .with_color(Color::Red),
                )
                .finish()
                .eprint(("query", Source::from(&args.query)))
                .unwrap();
        }
    }

    Ok(())
}

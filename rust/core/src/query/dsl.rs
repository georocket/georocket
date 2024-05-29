use chumsky::{prelude::*, text::whitespace};

use crate::index::Value;

use super::{Logic, Operator, Query, QueryPart};

pub fn compile_query(query: &str) -> Result<Query, Vec<Rich<'_, char>>> {
    let p = parser();
    p.parse(query).into_result()
}

fn parser<'a>() -> impl Parser<'a, &'a str, Query, extra::Err<Rich<'a, char>>> {
    recursive(|query_parts| {
        let string = any()
            .filter(|c: &char| !c.is_whitespace() && *c != ')')
            .repeated()
            .at_least(1)
            .collect();

        let double_quoted_string = choice((just("\\\"").to('"'), none_of('"')))
            .repeated()
            .collect()
            .delimited_by(just('"'), just('"'));

        let single_quoted_string = choice((just("\\'").to('\''), none_of('\'')))
            .repeated()
            .collect()
            .delimited_by(just('\''), just('\''));

        let frac_with_digits = just('.').then(text::digits(10));

        let frac_with_optional_digits = just('.').then(text::digits(10).or_not());

        let exp = one_of("eE")
            .then(one_of("+-").or_not())
            .then(text::digits(10));

        let number = one_of("+-")
            .or_not()
            .then(choice((
                frac_with_digits.to_slice(),
                text::int(10)
                    .then(frac_with_optional_digits.or_not())
                    .to_slice(),
            )))
            .then(exp.or_not());

        // strings starting with numbers are also valid, so we must make sure
        // the number is followed by a whitespace, a closing parenthesis,
        // or that it's the last element
        let number_value = number
            .then_ignore(choice((whitespace().exactly(1), just(')').ignored(), end())).rewind())
            .to_slice();

        let and = just("AND").ignore_then(whitespace()).ignore_then(
            query_parts
                .clone()
                .delimited_by(just('('), just(')'))
                .map(|a| QueryPart::Logical(Logic::And(a))),
        );

        let or = just("OR").ignore_then(whitespace()).ignore_then(
            query_parts
                .clone()
                .delimited_by(just('('), just(')'))
                .map(|a| QueryPart::Logical(Logic::Or(a))),
        );

        let not = just("NOT").ignore_then(whitespace()).ignore_then(
            query_parts
                .clone()
                .delimited_by(just('('), just(')'))
                .map(|a| QueryPart::Logical(Logic::Not(a))),
        );

        let key_value = choice((double_quoted_string, single_quoted_string, string))
            .then_ignore(whitespace().at_least(1))
            .then(choice((
                double_quoted_string.map(|a: String| a.into()),
                single_quoted_string.map(|a: String| a.into()),
                number_value.map(|a: &str| {
                    a.parse::<i64>()
                        .map(|i| i.into())
                        .unwrap_or_else(|_| a.parse::<f64>().unwrap().into())
                }),
                string.map(|a: String| a.into()),
            )))
            .padded();

        let eq = just("EQ").ignore_then(whitespace()).ignore_then(
            key_value
                .delimited_by(just('('), just(')'))
                .map(|(key, value): (String, Value)| QueryPart::Comparison {
                    operator: Operator::Eq,
                    key,
                    value,
                }),
        );

        let gt = just("GT").ignore_then(whitespace()).ignore_then(
            key_value
                .delimited_by(just('('), just(')'))
                .map(|(key, value): (String, Value)| QueryPart::Comparison {
                    operator: Operator::Gt,
                    key,
                    value,
                }),
        );

        let gte = just("GTE").ignore_then(whitespace()).ignore_then(
            key_value
                .delimited_by(just('('), just(')'))
                .map(|(key, value): (String, Value)| QueryPart::Comparison {
                    operator: Operator::Gte,
                    key,
                    value,
                }),
        );

        let lt = just("LT").ignore_then(whitespace()).ignore_then(
            key_value
                .delimited_by(just('('), just(')'))
                .map(|(key, value): (String, Value)| QueryPart::Comparison {
                    operator: Operator::Lt,
                    key,
                    value,
                }),
        );

        let lte = just("LTE").ignore_then(whitespace()).ignore_then(
            key_value
                .delimited_by(just('('), just(')'))
                .map(|(key, value): (String, Value)| QueryPart::Comparison {
                    operator: Operator::Lte,
                    key,
                    value,
                }),
        );

        let query = choice((
            and,
            or,
            not,
            eq,
            gt,
            gte,
            lt,
            lte,
            double_quoted_string.map(Into::into),
            single_quoted_string.map(Into::into),
            number_value.map(|n: &str| n.parse::<f64>().unwrap().into()),
            string.map(Into::into),
        ))
        .separated_by(whitespace().at_least(1))
        .collect()
        .padded();

        query
    })
    .then_ignore(end())
    .map(Vec::<QueryPart>::into)
}

#[cfg(test)]
mod tests {
    use super::compile_query;
    use crate::query::{and, eq, gt, gte, lt, lte, not, or, query};

    #[test]
    fn string() {
        let expected = query!["bla"];

        assert_eq!(compile_query("bla").unwrap(), expected);
        assert_eq!(compile_query("bla  ").unwrap(), expected);
        assert_eq!(compile_query("  bla").unwrap(), expected);
        assert_eq!(compile_query("   bla  ").unwrap(), expected);
    }

    #[test]
    fn strings() {
        let expected = query!["foo", "bar"];

        assert_eq!(compile_query("foo bar").unwrap(), expected);
        assert_eq!(compile_query("  foo  bar").unwrap(), expected);
        assert_eq!(compile_query("    foo    bar      ").unwrap(), expected);
    }

    #[test]
    fn strings_with_digits() {
        assert_eq!(
            compile_query("a14 1a  5a5 a5a").unwrap(),
            query!["a14", "1a", "5a5", "a5a"]
        );

        assert_eq!(compile_query("abc 1a").unwrap(), query!["abc", "1a"]);
    }

    #[test]
    fn double_quoted_string() {
        let expected = query!["foo bar"];

        assert_eq!(compile_query("\"foo bar\"").unwrap(), expected);
        assert_eq!(compile_query("\"foo bar\"  ").unwrap(), expected);
        assert_eq!(compile_query("  \"foo bar\"").unwrap(), expected);
        assert_eq!(compile_query("   \"foo bar\"  ").unwrap(), expected);
    }

    #[test]
    fn double_quoted_string_escape() {
        assert_eq!(
            compile_query("\"foo \\\" bar\"").unwrap(),
            query!["foo \" bar"]
        );
    }

    #[test]
    fn double_quoted_strings() {
        assert_eq!(
            compile_query("   \"foo bar\"   \"\"  \"Elvis \" ").unwrap(),
            query!["foo bar", "", "Elvis "]
        );
    }

    #[test]
    fn single_quoted_string() {
        let expected = query!["foo bar"];

        assert_eq!(compile_query("'foo bar'").unwrap(), expected);
        assert_eq!(compile_query("'foo bar'  ").unwrap(), expected);
        assert_eq!(compile_query("  'foo bar'").unwrap(), expected);
        assert_eq!(compile_query("   'foo bar'  ").unwrap(), expected);
    }

    #[test]
    fn single_quoted_string_escape() {
        assert_eq!(
            compile_query("'That\\'s nice'").unwrap(),
            query!["That's nice"]
        );
    }

    #[test]
    fn single_quoted_strings() {
        assert_eq!(
            compile_query("   'foo bar'   ''  'Elvis ' ").unwrap(),
            query!["foo bar", "", "Elvis "]
        );
    }

    #[test]
    fn mixed_strings() {
        assert_eq!(
            compile_query(
                " \"foo 5 bar\"   abcd  \"Another string\"  'single' \"That's nice\" end"
            )
            .unwrap(),
            query![
                "foo 5 bar",
                "abcd",
                "Another string",
                "single",
                "That's nice",
                "end",
            ]
        );
    }

    #[test]
    fn number() {
        assert_eq!(compile_query("13").unwrap(), query![13.]);
    }

    #[test]
    fn numbers() {
        assert_eq!(
            compile_query("13 1. -1 +20 1.5 -2.43 .05 1e5 2e-2 10.4e10 -10.3E-5 .4e4 .4E-4 2.E-5")
                .unwrap(),
            query![
                13., 1., -1., 20., 1.5, -2.43, 0.05, 1e5, 2e-2, 10.4e10, -10.3E-5, 0.4e4, 0.4E-4,
                2.0E-5,
            ]
        );
    }

    #[test]
    fn quoted_or() {
        assert_eq!(
            compile_query("\"OR(foo bar)\"").unwrap(),
            query!["OR(foo bar)"]
        );
    }

    #[test]
    fn and_strings() {
        assert_eq!(
            compile_query("AND(foo bar)").unwrap(),
            query![and!["foo", "bar"]]
        );
    }

    #[test]
    fn and_string_number() {
        assert_eq!(
            compile_query("AND(foo 5)").unwrap(),
            query![and!["foo", 5.0]]
        );
    }

    #[test]
    fn or() {
        assert_eq!(
            compile_query("OR(foo bar)").unwrap(),
            query![or!["foo", "bar"]]
        );
    }

    #[test]
    fn not() {
        assert_eq!(
            compile_query("NOT(foo bar)").unwrap(),
            query![not!["foo", "bar"]]
        );
    }

    #[test]
    fn eq() {
        assert_eq!(
            compile_query("EQ(foo bar)").unwrap(),
            query![eq!["foo", "bar"]]
        );
        assert_eq!(
            compile_query("EQ  (  foo bar   )  ").unwrap(),
            query![eq!["foo", "bar"]]
        );
        assert_eq!(
            compile_query("EQ(foo \"bar\")").unwrap(),
            query![eq!["foo", "bar"]]
        );
        assert_eq!(
            compile_query("EQ(\"foo\" \"bar\")").unwrap(),
            query![eq!["foo", "bar"]]
        );
        assert_eq!(
            compile_query("EQ('foo' bar)").unwrap(),
            query![eq!["foo", "bar"]]
        );
        assert_eq!(
            compile_query("EQ('foo' 'bar')").unwrap(),
            query![eq!["foo", "bar"]]
        );
        assert_eq!(compile_query("EQ(foo 4)").unwrap(), query![eq!["foo", 4]]);
    }

    #[test]
    fn gt() {
        assert_eq!(
            compile_query("GT(height 12)").unwrap(),
            query![gt!["height", 12]]
        );
    }

    #[test]
    fn gte() {
        assert_eq!(
            compile_query("GTE(height 12.0  )").unwrap(),
            query![gte!["height", 12.]]
        );
    }

    #[test]
    fn lt() {
        assert_eq!(
            compile_query("LT(height -12.)").unwrap(),
            query![lt!["height", -12.]]
        );
    }

    #[test]
    fn lte() {
        assert_eq!(
            compile_query("LTE(height -12e5)").unwrap(),
            query![lte!["height", -12e5]]
        );
    }

    #[test]
    fn not_eq() {
        assert_eq!(
            compile_query("NOT(EQ(foo bar))").unwrap(),
            query![not![eq!["foo", "bar"]]]
        );
    }

    #[test]
    fn complex() {
        assert_eq!(
            compile_query("AND(foo bar OR(test1 test2)) hello NOT(world \"you\")").unwrap(),
            query![
                and!["foo", "bar", or!["test1", "test2"]],
                "hello",
                not!["world", "you"]
            ]
        );
    }
}

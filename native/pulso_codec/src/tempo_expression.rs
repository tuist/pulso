//! Bounded same-parent boolean program. No projection/type inference, DNF
//! expansion, short-circuit accounting, or cross-span joins.
use crate::json_read::{Parser, RawFields};
use crate::trace_search::{field, object, string, Error, Res};
use std::collections::BTreeMap;

const WORK_MAX: usize = 20_000_000;
pub(crate) type Attributes = BTreeMap<(String, String), Vec<u8>>;
pub(crate) struct Expression(Vec<Instruction>);
enum Instruction {
    And,
    Or,
    Leaf {
        scope: String,
        key: String,
        op: String,
        wanted: String,
        regex: Option<regex::Regex>,
    },
}
pub(crate) struct Evaluation {
    pub(crate) matched: bool,
    pub(crate) attributes: Attributes,
}
impl Expression {
    pub(crate) fn compile(program: Vec<(String, String, String, String)>) -> Option<Self> {
        if program.is_empty() || program.len() > 31 {
            return None;
        }
        let (mut leaves, mut regexes, mut stack) = (0, 0, 0);
        let mut out = Vec::with_capacity(program.len());
        for (scope, key, op, wanted) in program {
            if scope == "boolean" {
                if stack < 2 || !key.is_empty() || !wanted.is_empty() {
                    return None;
                }
                out.push(match op.as_str() {
                    "&&" => Instruction::And,
                    "||" => Instruction::Or,
                    _ => return None,
                });
                stack -= 1;
                continue;
            }
            leaves += 1;
            stack += 1;
            if leaves > 16
                || key.is_empty()
                || key.len() > 256
                || wanted.len() > 4096
                || wanted.contains("${")
            {
                return None;
            }
            let re = match scope.as_str() {
                "resource.string" | "span.string" | "none.string" => match op.as_str() {
                    "=" | "!=" => None,
                    "=~" | "!~" => {
                        regexes += 1;
                        if regexes > 8 || wanted.len() > 1024 {
                            return None;
                        }
                        Some(crate::query_regex::anchored(&wanted).ok()?)
                    }
                    _ => return None,
                },
                "resource.int" | "span.int" | "none.int" | "intrinsic" => {
                    if !["=", "!=", "<", "<=", ">", ">="].contains(&op.as_str()) {
                        return None;
                    }
                    let n = wanted.parse::<i64>().ok()?;
                    if scope == "intrinsic"
                        && match key.as_str() {
                            "status" => {
                                !["=", "!="].contains(&op.as_str()) || !(0..=2).contains(&n)
                            }
                            "duration" => n < 0,
                            _ => true,
                        }
                    {
                        return None;
                    }
                    None
                }
                _ => return None,
            };
            out.push(Instruction::Leaf {
                scope,
                key,
                op,
                wanted,
                regex: re,
            });
        }
        (stack == 1).then_some(Self(out))
    }

    pub(crate) fn evaluate(
        &self,
        fields: &RawFields<'_>,
        work: &mut usize,
        collect: bool,
    ) -> Res<Evaluation> {
        let meta = object(field(fields, b"otlp")?)?;
        let resource = attributes(field(&meta, b"resource")?)?;
        let span = attributes(field(&meta, b"attributes")?)?;
        // Charge all owners, attributes and instructions before filtering, in
        // both candidate and retained-context passes, including nonmatches.
        *work = work
            .checked_add(2 + resource.len() + span.len() + self.0.len())
            .ok_or(Error::Budget)?;
        if *work > WORK_MAX {
            return Err(Error::Budget);
        }
        let mut stack = Vec::with_capacity(16);
        let mut attrs = BTreeMap::new();
        for instruction in &self.0 {
            match instruction {
                Instruction::And | Instruction::Or => {
                    let rhs = stack.pop().ok_or(Error::Invalid)?;
                    let lhs = stack.pop().ok_or(Error::Invalid)?;
                    stack.push(if matches!(instruction, Instruction::And) {
                        lhs && rhs
                    } else {
                        lhs || rhs
                    });
                }
                Instruction::Leaf {
                    scope,
                    key,
                    op,
                    wanted,
                    regex,
                } => {
                    let (matched, value) = if scope == "intrinsic" {
                        let n = if key == "status" {
                            crate::tempo_search::integer(field(
                                &object(field(fields, b"status")?)?,
                                b"code",
                            )?)?
                        } else {
                            crate::tempo_search::integer(field(fields, b"end_timestamp_ns")?)?
                                - crate::tempo_search::integer(field(fields, b"timestamp_ns")?)?
                        };
                        let value = if collect && key == "status" {
                            let status = ["unset", "ok", "error"]
                                .get(n as usize)
                                .ok_or(Error::Invalid)?;
                            Some(format!("{{\"stringValue\":\"{status}\"}}").into_bytes())
                        } else {
                            None
                        };
                        (
                            compare(n, wanted.parse().map_err(|_| Error::Invalid)?, op),
                            value,
                        )
                    } else if scope.starts_with("none.") {
                        let string_type = scope.ends_with(".string");
                        if collect {
                            for (name, owner) in [("resource", &resource), ("span", &span)] {
                                if let Some(raw) = owner.get(key) {
                                    if attribute_matches(
                                        raw,
                                        string_type,
                                        op,
                                        wanted,
                                        regex.as_ref(),
                                    )? {
                                        attrs.insert((key.clone(), name.to_owned()), raw.to_vec());
                                    }
                                }
                            }
                        }
                        // An original span value wins even if its type/value
                        // does not match. Samples above retain both owners.
                        let matched = span
                            .get(key)
                            .or_else(|| resource.get(key))
                            .map_or(Ok(false), |raw| {
                                attribute_matches(raw, string_type, op, wanted, regex.as_ref())
                            })?;
                        (matched, None)
                    } else {
                        let owner = if scope.starts_with("resource.") {
                            &resource
                        } else {
                            &span
                        };
                        match owner.get(key) {
                            None => (false, None),
                            Some(raw) => {
                                let matched = attribute_matches(
                                    raw,
                                    scope.ends_with(".string"),
                                    op,
                                    wanted,
                                    regex.as_ref(),
                                )?;
                                (matched, (collect && matched).then(|| raw.to_vec()))
                            }
                        }
                    };
                    if matched {
                        if let Some(value) = value {
                            let owner = scope.split('.').next().unwrap_or(scope);
                            attrs.insert((key.clone(), owner.to_owned()), value);
                        }
                    }
                    stack.push(matched);
                }
            }
        }
        Ok(Evaluation {
            matched: stack.pop().ok_or(Error::Invalid)?,
            attributes: attrs,
        })
    }
}
fn attribute_matches(
    raw: &[u8],
    string_type: bool,
    op: &str,
    wanted: &str,
    regex: Option<&regex::Regex>,
) -> Res<bool> {
    let value = object(raw)?;
    if string_type {
        match field(&value, b"stringValue") {
            Ok(text) => {
                let text = string(text)?;
                let positive = regex.map_or(text == wanted, |r| r.is_match(&text));
                Ok(if op == "!=" || op == "!~" {
                    !positive
                } else {
                    positive
                })
            }
            Err(_) => Ok(false),
        }
    } else {
        match field(&value, b"intValue") {
            Ok(n) => Ok(compare(
                string(n)?.parse().map_err(|_| Error::Invalid)?,
                wanted.parse().map_err(|_| Error::Invalid)?,
                op,
            )),
            Err(_) => Ok(false),
        }
    }
}
pub(crate) fn attributes(raw: &[u8]) -> Res<BTreeMap<String, &[u8]>> {
    let mut parser = Parser::new(raw);
    let mut out = BTreeMap::new();
    for pair in parser.raw_array().map_err(|_| Error::Invalid)? {
        let pair = object(pair)?;
        let key = string(field(&pair, b"key")?)?;
        if out.insert(key, field(&pair, b"value")?).is_some() {
            return Err(Error::Invalid);
        }
    }
    Ok(out)
}
fn compare(left: i64, right: i64, op: &str) -> bool {
    match op {
        "=" => left == right,
        "!=" => left != right,
        "<" => left < right,
        "<=" => left <= right,
        ">" => left > right,
        ">=" => left >= right,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn leaf(scope: &str, key: &str, op: &str, value: &str) -> (String, String, String, String) {
        (scope.into(), key.into(), op.into(), value.into())
    }
    #[test]
    fn malformed_programs_cannot_bypass_the_frontend() {
        for program in [
            vec![],
            vec![leaf("boolean", "", "||", "")],
            vec![leaf("span.int", "code", "=", "9223372036854775808")],
            vec![leaf("intrinsic", "duration", ">", "-1")],
            vec![leaf("intrinsic", "status", "=", "3")],
            vec![leaf("intrinsic", "status", ">", "1")],
            vec![leaf("span.string", "route", "!~", "(?=x)")],
            vec![leaf("span.string", "route", "!~", "${route}")],
            vec![leaf("span.int", "code", "=", "1"); 32],
        ] {
            assert!(Expression::compile(program).is_none());
        }
    }
    #[test]
    fn negative_regex_requires_original_string_and_work_is_charged_before_truth() {
        let expression =
            Expression::compile(vec![leaf("span.string", "route", "!~", "/health")]).unwrap();
        for (value, expected) in [
            (r#"{"stringValue":"/health"}"#, false),
            (r#"{"stringValue":"/work"}"#, true),
            (r#"{"intValue":"503"}"#, false),
            (r#"{"boolValue":true}"#, false),
        ] {
            let payload = format!(
                r#"{{"otlp":{{"resource":[],"attributes":[{{"key":"route","value":{value}}}]}}}}"#
            );
            let fields = object(payload.as_bytes()).unwrap();
            let mut work = 0;
            assert_eq!(
                expression
                    .evaluate(&fields, &mut work, true)
                    .unwrap()
                    .matched,
                expected
            );
            assert_eq!(work, 4);
            work = WORK_MAX - 3;
            assert!(matches!(
                expression.evaluate(&fields, &mut work, false),
                Err(Error::Budget)
            ));
        }
        let fields = object(br#"{"otlp":{"resource":[],"attributes":[]}}"#).unwrap();
        assert!(!expression.evaluate(&fields, &mut 0, false).unwrap().matched);
    }
    #[test]
    fn int64_comparison_is_exact_and_nonmatching_or_attributes_are_not_invented() {
        let expression = Expression::compile(vec![
            leaf("intrinsic", "status", "=", "2"),
            leaf("span.int", "code", "=", "9007199254740993"),
            leaf("boolean", "", "||", ""),
        ])
        .unwrap();
        let fields = object(br#"{"status":{"code":0},"otlp":{"resource":[],"attributes":[{"key":"code","value":{"intValue":"9007199254740993"}}]}}"#).unwrap();
        let candidate = expression.evaluate(&fields, &mut 0, false).unwrap();
        assert!(candidate.matched);
        assert!(candidate.attributes.is_empty());
        let got = expression.evaluate(&fields, &mut 0, true).unwrap();
        assert!(got.matched);
        assert_eq!(got.attributes.len(), 1);
        assert!(got.attributes.contains_key(&("code".into(), "span".into())));
    }
}

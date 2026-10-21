//! Shared bounded Go/RE2-subset regex policy for metric and log queries.
//! Perl classes and word boundaries are ASCII; ordinary matching, including
//! `.`, remains Unicode/codepoint-aware. Reject Rust-only class extensions.

const PROGRAM_BYTES: usize = 1_048_576;

pub fn string(pattern: &str) -> Result<regex::Regex, regex::Error> {
    regex::RegexBuilder::new(&translate(pattern)?)
        .size_limit(PROGRAM_BYTES)
        .dfa_size_limit(PROGRAM_BYTES)
        .build()
}

pub fn bytes(pattern: &str) -> Result<regex::bytes::Regex, regex::Error> {
    regex::bytes::RegexBuilder::new(&translate(pattern)?)
        .size_limit(PROGRAM_BYTES)
        .dfa_size_limit(PROGRAM_BYTES)
        .build()
}

fn translate(pattern: &str) -> Result<String, regex::Error> {
    let normalized = leading_class_literals(pattern);
    reject_class_extensions(&normalized)?;
    let mut translated = String::with_capacity(normalized.len());
    let mut chars = normalized.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            translated.push(ch);
            continue;
        }
        match chars.next() {
            Some('d') => translated.push_str("[0-9]"),
            Some('D') => translated.push_str("[^0-9]"),
            Some('w') => translated.push_str("[A-Za-z0-9_]"),
            Some('W') => translated.push_str("[^A-Za-z0-9_]"),
            Some('s') => translated.push_str(r"[\t\n\f\r ]"),
            Some('S') => translated.push_str(r"[^\t\n\f\r ]"),
            Some('b') => translated.push_str(r"(?-u:\b)"),
            Some('B') => translated.push_str(r"(?-u:\B)"),
            Some(next @ ('<' | '>')) => translated.push(next),
            Some(next) => {
                translated.push('\\');
                translated.push(next);
            }
            None => translated.push('\\'),
        }
    }
    Ok(translated)
}

// Go/PCRE allow a literal ] first in a class (after optional ^); Rust
// interprets it as an empty class. Escape only that lexical position.
fn leading_class_literals(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        out.push(ch);
        if ch == '\\' {
            if let Some(next) = chars.next() {
                out.push(next);
            }
        } else if ch == '[' {
            if chars.peek() == Some(&'^') {
                out.push(chars.next().unwrap());
            }
            if chars.peek() == Some(&']') {
                chars.next();
                out.push_str(r"\]");
            }
        }
    }
    out
}

fn reject_class_extensions(pattern: &str) -> Result<(), regex::Error> {
    let mut depth = 0usize;
    let mut escaped = false;
    let mut previous = '\0';
    let mut active_counter = false;
    let mut previous_counter = false;
    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        if escaped {
            escaped = false;
            previous = '\0';
            previous_counter = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            previous = '\0';
            previous_counter = false;
            continue;
        }
        if depth == 0 && ch == '(' {
            let mut flags = chars.clone();
            if flags.next() == Some('?') {
                for flag in flags {
                    match flag {
                        'i' | 'm' | 's' | 'U' | '-' => (),
                        'u' | 'x' | 'R' => {
                            return Err(regex::Error::Syntax("Unsupported regex flag".into()))
                        }
                        _ => break,
                    }
                }
            }
        }
        if depth == 0 && ch == '{' {
            active_counter = repetition_counter(chars.clone());
            if active_counter && (matches!(previous, '*' | '+' | '?') || previous_counter) {
                return Err(regex::Error::Syntax("Unsupported nested repetition".into()));
            }
        }
        if depth == 0
            && matches!(ch, '*' | '+')
            && (matches!(previous, '*' | '+' | '?') || previous_counter)
        {
            return Err(regex::Error::Syntax(
                "Unsupported nested/possessive repetition".into(),
            ));
        }
        if (depth > 0 && ch == previous && matches!(ch, '&' | '-' | '~'))
            || (depth > 0 && ch == '[' && !posix_class(chars.clone()))
        {
            return Err(regex::Error::Syntax(
                "Unsupported character-class extension".into(),
            ));
        }
        if ch == '[' {
            depth += 1;
        }
        if ch == ']' {
            depth = depth.saturating_sub(1);
        }
        previous_counter = ch == '}' && active_counter;
        if ch == '}' {
            active_counter = false;
        }
        previous = ch;
    }
    Ok(())
}

fn repetition_counter(mut chars: impl Iterator<Item = char>) -> bool {
    if !chars.next().is_some_and(|ch| ch.is_ascii_digit()) {
        return false;
    }
    let mut comma = false;
    for ch in chars {
        match ch {
            '0'..='9' => (),
            ',' if !comma => comma = true,
            '}' => return true,
            _ => return false,
        }
    }
    false
}

fn posix_class(chars: impl Iterator<Item = char>) -> bool {
    let prefix: String = chars.take(12).collect();
    let Some(body) = prefix.strip_prefix(':') else {
        return false;
    };
    let body = body.strip_prefix('^').unwrap_or(body);
    let Some((name, _)) = body.split_once(":]") else {
        return false;
    };
    [
        "alnum", "alpha", "ascii", "blank", "cntrl", "digit", "graph", "lower", "print", "punct",
        "space", "upper", "word", "xdigit",
    ]
    .contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_and_string_engines_share_codepoints_ascii_classes_and_strict_end() {
        for pattern in [
            "(?u).", "(?ix)a", "(?R)$", "[a[:b]]", r"\Q\d\E", r"[a[]\d", "^a*+a$", "a{2}+", "a**",
            "a{2}{3}", "a*{2}", "a+{2}", "a?{1}",
        ] {
            assert!(string(pattern).is_err());
            assert!(bytes(pattern).is_err());
        }
        for (pattern, value, expected) in [
            (r"^.$", "é", true),
            (r"^\d$", "٣", false),
            (r"^\w$", "é", false),
            (r"\bfoo\b", "éfooé", true),
            (r"^.$", "a\n", false),
            (r"(?s)^.$", "\n", true),
            (r"^\s$", "\u{b}", false),
            (r"^[\S]$", "\u{b}", true),
            (r"(?i)^[\W]$", "K", false),
            (r"\<foo", "a foo", false),
            (r"\<foo", "<foo", true),
            (r"^[[:alpha:]]$", "a", true),
            (r"^[^]]$", "a", true),
            (r"^[^]]$", "]", false),
            (r"^[^]\d]$", "a", true),
            (r"^[a^]$", "^", true),
            (r"^a}*$", "a}}", true),
            (r"^\{2}*$", "{2}}", true),
            (r"^[\W^]$", "b", false),
            (r"^[\W^]$", "^", true),
            (r"^a{2}\*+$", "aa***", true),
            (r"^a{2}\d+$", "aa123", true),
        ] {
            assert_eq!(string(pattern).unwrap().is_match(value), expected);
            assert_eq!(bytes(pattern).unwrap().is_match(value.as_bytes()), expected);
        }
    }
}

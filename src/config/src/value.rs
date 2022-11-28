// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Hosts the [`Value`] trait and its implementations.

use std::time::Duration;

use mz_sql::ast::{Ident, SetVariableValue, Value as AstValue};
use mz_sql_parser::parser::parse_set_variable_value;

/// A value that can be stored in a session or server variable.
pub trait Value: ToOwned + Send + Sync + PartialEq + Eq {
    /// The name of the value type.
    const TYPE_NAME: &'static str;
    /// Parses a value of this type from a string.
    fn parse(s: &str) -> Result<Self::Owned, ()>;
    /// Formats this value as a string.
    fn format(&self) -> String;
}

impl Value for bool {
    const TYPE_NAME: &'static str = "boolean";

    fn parse(s: &str) -> Result<Self, ()> {
        match s {
            "t" | "true" | "on" => Ok(true),
            "f" | "false" | "off" => Ok(false),
            _ => Err(()),
        }
    }

    fn format(&self) -> String {
        match self {
            true => "on".into(),
            false => "off".into(),
        }
    }
}

impl Value for i32 {
    const TYPE_NAME: &'static str = "integer";

    fn parse(s: &str) -> Result<i32, ()> {
        s.parse().map_err(|_| ())
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for u32 {
    const TYPE_NAME: &'static str = "unsigned integer";

    fn parse(s: &str) -> Result<u32, ()> {
        s.parse().map_err(|_| ())
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

const SEC_TO_MIN: u64 = 60u64;
const SEC_TO_HOUR: u64 = 60u64 * 60;
const SEC_TO_DAY: u64 = 60u64 * 60 * 24;
const MICRO_TO_MILLI: u32 = 1000u32;

impl Value for Duration {
    const TYPE_NAME: &'static str = "duration";

    fn parse(s: &str) -> Result<Duration, ()> {
        let s = s.trim();
        // Take all numeric values from [0..]
        let split_pos = s
            .chars()
            .position(|p| !char::is_numeric(p))
            .unwrap_or_else(|| s.chars().count());

        // Error if the numeric values don't parse, i.e. there aren't any.
        let d = s[..split_pos].parse::<u64>().map_err(|_| ())?;

        // We've already trimmed end
        let (f, m): (fn(u64) -> Duration, u64) = match s[split_pos..].trim_start() {
            "us" => (Duration::from_micros, 1),
            // Default unit is milliseconds
            "ms" | "" => (Duration::from_millis, 1),
            "s" => (Duration::from_secs, 1),
            "min" => (Duration::from_secs, SEC_TO_MIN),
            "h" => (Duration::from_secs, SEC_TO_HOUR),
            "d" => (Duration::from_secs, SEC_TO_DAY),
            _ => return Err(()),
        };

        let d = if d == 0 {
            Duration::from_secs(u64::MAX)
        } else {
            f(d.checked_mul(m).ok_or(())?)
        };
        Ok(d)
    }

    // The strategy for formatting these strings is to find the least
    // significant unit of time that can be printed as an integer––we know this
    // is always possible because the input can only be an integer of a single
    // unit of time.
    fn format(&self) -> String {
        let micros = self.subsec_micros();
        if micros > 0 {
            match micros {
                ms if ms != 0 && ms % MICRO_TO_MILLI == 0 => {
                    format!(
                        "{} ms",
                        self.as_secs() * 1000 + u64::from(ms / MICRO_TO_MILLI)
                    )
                }
                us => format!("{} us", self.as_secs() * 1_000_000 + u64::from(us)),
            }
        } else {
            match self.as_secs() {
                zero if zero == u64::MAX => "0".to_string(),
                d if d != 0 && d % SEC_TO_DAY == 0 => format!("{} d", d / SEC_TO_DAY),
                h if h != 0 && h % SEC_TO_HOUR == 0 => format!("{} h", h / SEC_TO_HOUR),
                m if m != 0 && m % SEC_TO_MIN == 0 => format!("{} min", m / SEC_TO_MIN),
                s => format!("{} s", s),
            }
        }
    }
}

#[test]
fn test_value_duration() {
    fn inner(t: &'static str, e: Duration, expected_format: Option<&'static str>) {
        let d = Duration::parse(t).unwrap();
        assert_eq!(d, e);
        let mut d_format = d.format();
        d_format.retain(|c| !c.is_whitespace());
        if let Some(expected) = expected_format {
            assert_eq!(d_format, expected);
        } else {
            assert_eq!(
                t.chars().filter(|c| !c.is_whitespace()).collect::<String>(),
                d_format
            )
        }
    }
    inner("1", Duration::from_millis(1), Some("1ms"));
    inner("0", Duration::from_secs(u64::MAX), None);
    inner("1ms", Duration::from_millis(1), None);
    inner("1000ms", Duration::from_millis(1000), Some("1s"));
    inner("1001ms", Duration::from_millis(1001), None);
    inner("1us", Duration::from_micros(1), None);
    inner("1000us", Duration::from_micros(1000), Some("1ms"));
    inner("1s", Duration::from_secs(1), None);
    inner("60s", Duration::from_secs(60), Some("1min"));
    inner("3600s", Duration::from_secs(3600), Some("1h"));
    inner("3660s", Duration::from_secs(3660), Some("61min"));
    inner("1min", Duration::from_secs(1 * SEC_TO_MIN), None);
    inner("60min", Duration::from_secs(60 * SEC_TO_MIN), Some("1h"));
    inner("1h", Duration::from_secs(1 * SEC_TO_HOUR), None);
    inner("24h", Duration::from_secs(24 * SEC_TO_HOUR), Some("1d"));
    inner("1d", Duration::from_secs(1 * SEC_TO_DAY), None);
    inner("2d", Duration::from_secs(2 * SEC_TO_DAY), None);
    inner("  1   s ", Duration::from_secs(1), None);
    inner("1s ", Duration::from_secs(1), None);
    inner("   1s", Duration::from_secs(1), None);
    inner("0s", Duration::from_secs(u64::MAX), Some("0"));
    inner("0d", Duration::from_secs(u64::MAX), Some("0"));
    inner(
        "18446744073709551615",
        Duration::from_millis(u64::MAX),
        Some("18446744073709551615ms"),
    );
    inner(
        "18446744073709551615 s",
        Duration::from_secs(u64::MAX),
        Some("0"),
    );

    fn errs(t: &'static str) {
        assert!(Duration::parse(t).is_err());
    }
    errs("1 m");
    errs("1 sec");
    errs("1 min 1 s");
    errs("1m1s");
    errs("1.1");
    errs("1.1 min");
    errs("-1 s");
    errs("");
    errs("   ");
    errs("x");
    errs("s");
    errs("18446744073709551615 min");
}

impl Value for str {
    const TYPE_NAME: &'static str = "string";

    fn parse(s: &str) -> Result<String, ()> {
        Ok(s.to_owned())
    }

    fn format(&self) -> String {
        self.to_owned()
    }
}

impl Value for [String] {
    const TYPE_NAME: &'static str = "string list";

    fn parse(s: &str) -> Result<Vec<String>, ()> {
        // Only supporting a single value for now, setting multiple values
        // requires a change in the parser.
        Ok(vec![s.to_owned()])
    }

    fn format(&self) -> String {
        self.join(", ")
    }
}

impl Value for Vec<String> {
    const TYPE_NAME: &'static str = "string list";

    fn parse(s: &str) -> Result<Vec<String>, ()> {
        if s.is_empty() {
            Ok(vec![]) // The empty vector might be a valid default and should be allowed.
        } else {
            match parse_set_variable_value(s) {
                Ok(SetVariableValue::Ident(ident)) => {
                    let value = vec![ident.into_string()];
                    Ok(value)
                }
                Ok(SetVariableValue::Literal(value)) => {
                    // Don't assume that value matches AstValue::String(...) here, as
                    // a single string might be wrongly parsed as a non-string literal.
                    let value = vec![value.to_string()];
                    Ok(value)
                }
                Ok(SetVariableValue::Literals(values)) => values
                    .into_iter()
                    .map(|value| match value {
                        AstValue::String(value) => Ok(value),
                        _ => Err(()),
                    })
                    .collect(),
                _ => Err(()),
            }
        }
    }

    fn format(&self) -> String {
        let ast = match self.as_slice() {
            [value] => SetVariableValue::Ident(Ident::new(value)),
            _ => SetVariableValue::Literals(self.iter().cloned().map(AstValue::String).collect()),
        };
        ast.to_string()
    }
}

impl Value for Option<String> {
    const TYPE_NAME: &'static str = "optional string";

    fn parse(s: &str) -> Result<Self::Owned, ()> {
        match s {
            "" => Ok(None),
            _ => Ok(Some(s.into())),
        }
    }

    fn format(&self) -> String {
        match self {
            Some(s) => s.clone(),
            None => "".into(),
        }
    }
}

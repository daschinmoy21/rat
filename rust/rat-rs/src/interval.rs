//! Parses Spark interval strings such as `10 minutes`, `1 hour 30 minutes`,
//! `interval 2 seconds`, and `500 milliseconds` into milliseconds.

use anyhow::{Context, Result, bail};

pub fn parse_ms(input: &str) -> Result<i64> {
    let mut tokens = input.split_whitespace().peekable();
    if tokens
        .peek()
        .is_some_and(|t| t.eq_ignore_ascii_case("interval"))
    {
        tokens.next();
    }
    let mut total: i64 = 0;
    let mut any = false;
    while let Some(number) = tokens.next() {
        let unit = tokens
            .next()
            .with_context(|| format!("interval {input:?}: number {number:?} has no unit"))?;
        let n: i64 = number
            .parse()
            .with_context(|| format!("interval {input:?}: {number:?} is not an integer"))?;
        let unit_ms: i64 = match unit.to_ascii_lowercase().trim_end_matches('s') {
            "m" if unit.eq_ignore_ascii_case("ms") => 1,
            "millisecond" | "milli" => 1,
            "second" | "sec" | "" => 1_000,
            "minute" | "min" | "m" => 60_000,
            "hour" | "h" => 3_600_000,
            "day" | "d" => 86_400_000,
            "week" | "w" => 7 * 86_400_000,
            _ => bail!("interval {input:?}: unknown unit {unit:?}"),
        };
        total = n
            .checked_mul(unit_ms)
            .and_then(|v| total.checked_add(v))
            .with_context(|| format!("interval {input:?} overflows"))?;
        any = true;
    }
    if !any {
        bail!("empty interval {input:?}");
    }
    if total < 0 {
        bail!("interval {input:?} is negative");
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::parse_ms;

    #[test]
    fn units() {
        assert_eq!(parse_ms("10 minutes").unwrap(), 600_000);
        assert_eq!(parse_ms("interval 1 hour 30 minutes").unwrap(), 5_400_000);
        assert_eq!(parse_ms("2 seconds").unwrap(), 2_000);
        assert_eq!(parse_ms("1 second").unwrap(), 1_000);
        assert_eq!(parse_ms("500 milliseconds").unwrap(), 500);
        assert_eq!(parse_ms("250 ms").unwrap(), 250);
        assert_eq!(parse_ms("1 day").unwrap(), 86_400_000);
        assert!(parse_ms("").is_err());
        assert!(parse_ms("10").is_err());
        assert!(parse_ms("ten minutes").is_err());
        assert!(parse_ms("3 fortnights").is_err());
    }
}

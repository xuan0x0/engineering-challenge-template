//! Exact decimal-string to fixed-point conversion. Prices and quantities in the
//! output schema are fixed point with six decimals: 1_000_000 = 1 USD or 1
//! contract.

/// Number of fixed-point units in one whole unit.
pub const SCALE: i64 = 1_000_000;
const SCALE_DIGITS: usize = 6;

/// Parses a decimal string such as `"0.9600"` or `"-54.00"` into six-decimal
/// fixed point, without going through floating point. Fails rather than
/// silently truncating digits beyond the sixth decimal.
pub fn parse_fixed(s: &str) -> anyhow::Result<i64> {
    let (negative, unsigned) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    anyhow::ensure!(
        !(int_part.is_empty() && frac_part.is_empty()),
        "empty decimal {s:?}"
    );
    anyhow::ensure!(
        int_part
            .bytes()
            .chain(frac_part.bytes())
            .all(|b| b.is_ascii_digit()),
        "invalid decimal {s:?}"
    );

    let frac_trimmed = frac_part.trim_end_matches('0');
    anyhow::ensure!(
        frac_trimmed.len() <= SCALE_DIGITS,
        "decimal {s:?} has more than {SCALE_DIGITS} significant fractional digits"
    );

    let whole: i64 = if int_part.is_empty() {
        0
    } else {
        int_part.parse()?
    };
    let frac: i64 = if frac_trimmed.is_empty() {
        0
    } else {
        let padded = format!("{frac_trimmed:0<SCALE_DIGITS$}");
        padded.parse()?
    };
    let magnitude = whole
        .checked_mul(SCALE)
        .and_then(|w| w.checked_add(frac))
        .ok_or_else(|| anyhow::anyhow!("decimal {s:?} overflows fixed point"))?;
    Ok(if negative { -magnitude } else { magnitude })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_prices_and_quantities() {
        assert_eq!(parse_fixed("0.9600").unwrap(), 960_000);
        assert_eq!(parse_fixed("0.0012").unwrap(), 1_200);
        assert_eq!(parse_fixed("1").unwrap(), 1_000_000);
        assert_eq!(parse_fixed("54.00").unwrap(), 54_000_000);
        assert_eq!(parse_fixed("-54.00").unwrap(), -54_000_000);
        assert_eq!(parse_fixed("+3.5").unwrap(), 3_500_000);
        assert_eq!(parse_fixed(".5").unwrap(), 500_000);
        assert_eq!(parse_fixed("0.1234560000").unwrap(), 123_456);
    }

    #[test]
    fn rejects_malformed_or_lossy_input() {
        assert!(parse_fixed("").is_err());
        assert!(parse_fixed("-").is_err());
        assert!(parse_fixed("1.2.3").is_err());
        assert!(parse_fixed("abc").is_err());
        assert!(parse_fixed("0.1234567").is_err());
        assert!(parse_fixed("99999999999999999").is_err());
    }
}

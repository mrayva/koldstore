//! `date` and `timestamp` (without time zone) text conversion in PostgreSQL's own representation.
//!
//! Values are exchanged as PostgreSQL-epoch integers (`DateADT` days / `Timestamp` microseconds since
//! 2000-01-01) and as the ISO text PostgreSQL prints in `to_jsonb` and accepts back from
//! `jsonb_populate_record`: `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM:SS[.ffffff]`, an optional ` BC` suffix and
//! the literals `infinity` / `-infinity`. `infinity` is PostgreSQL's extreme integer value and is kept
//! as such so it never goes through an epoch shift.
//!
//! BC years follow PostgreSQL: `0001-01-01 BC` is astronomical year 0, so year `N BC` is `1 - N`.

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

const PG_INFINITY_TEXT: &str = "infinity";
const PG_NEG_INFINITY_TEXT: &str = "-infinity";

fn pg_epoch_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2000, 1, 1).expect("valid epoch")
}

/// Splits an optional ` BC` suffix off `text`.
fn split_bc(text: &str) -> (&str, bool) {
    match text.strip_suffix(" BC") {
        Some(rest) => (rest.trim_end(), true),
        None => (text, false),
    }
}

/// Parses `YYYY-MM-DD` (year padded to at least 4 digits) into a date, honoring BC.
fn parse_ymd(text: &str, bc: bool) -> Result<NaiveDate, String> {
    let mut parts = text.splitn(3, '-');
    let (Some(year), Some(month), Some(day)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(format!("invalid date literal `{text}`"));
    };
    let year: i32 = year
        .parse()
        .map_err(|_| format!("invalid year in date literal `{text}`"))?;
    let month: u32 = month
        .parse()
        .map_err(|_| format!("invalid month in date literal `{text}`"))?;
    let day: u32 = day
        .parse()
        .map_err(|_| format!("invalid day in date literal `{text}`"))?;
    // Astronomical year numbering: 1 BC is year 0.
    let year = if bc { 1 - year } else { year };
    NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| format!("date literal `{text}` is out of range"))
}

/// Parses PostgreSQL date text into days since 2000-01-01.
///
/// # Errors
///
/// Returns an error for text that is not a valid or representable date.
pub fn parse_date_pg_days(text: &str) -> Result<i32, String> {
    let text = text.trim();
    match text {
        PG_INFINITY_TEXT => return Ok(i32::MAX),
        PG_NEG_INFINITY_TEXT => return Ok(i32::MIN),
        _ => {}
    }
    let (body, bc) = split_bc(text);
    // Tolerate a full timestamp text for a date column: only the date part matters.
    let body = body.split(['T', ' ']).next().unwrap_or(body);
    let date = parse_ymd(body, bc)?;
    i32::try_from(date.signed_duration_since(pg_epoch_date()).num_days())
        .map_err(|_| format!("date `{text}` is out of range"))
}

/// Formats days since 2000-01-01 as PostgreSQL date text.
///
/// # Errors
///
/// Returns an error for dates outside the range this conversion can represent (about ±262,000 years).
pub fn format_date_pg_days(days: i32) -> Result<String, String> {
    match days {
        i32::MAX => return Ok(PG_INFINITY_TEXT.to_string()),
        i32::MIN => return Ok(PG_NEG_INFINITY_TEXT.to_string()),
        _ => {}
    }
    let date = pg_epoch_date()
        .checked_add_signed(Duration::days(i64::from(days)))
        .ok_or_else(|| format!("date value {days} is out of range"))?;
    Ok(format_ymd(date))
}

fn format_ymd(date: NaiveDate) -> String {
    use chrono::Datelike;
    let year = date.year();
    if year <= 0 {
        format!("{:04}-{:02}-{:02} BC", 1 - year, date.month(), date.day())
    } else {
        format!("{:04}-{:02}-{:02}", year, date.month(), date.day())
    }
}

/// Parses PostgreSQL `timestamp` (no zone) text into microseconds since 2000-01-01.
///
/// # Errors
///
/// Returns an error for text that is not a valid or representable timestamp.
pub fn parse_timestamp_pg_micros(text: &str) -> Result<i64, String> {
    let text = text.trim();
    match text {
        PG_INFINITY_TEXT => return Ok(i64::MAX),
        PG_NEG_INFINITY_TEXT => return Ok(i64::MIN),
        _ => {}
    }
    let (body, bc) = split_bc(text);
    let (date_text, time_text) = match body.split_once(['T', ' ']) {
        Some((date, time)) => (date, Some(time.trim())),
        None => (body, None),
    };
    let date = parse_ymd(date_text, bc)?;
    let time = match time_text {
        Some(time) if !time.is_empty() => NaiveTime::parse_from_str(time, "%H:%M:%S%.f")
            .or_else(|_| NaiveTime::parse_from_str(time, "%H:%M"))
            .map_err(|error| format!("invalid time in timestamp literal `{text}`: {error}"))?,
        _ => NaiveTime::from_hms_opt(0, 0, 0).expect("midnight"),
    };
    NaiveDateTime::new(date, time)
        .signed_duration_since(pg_epoch_date().and_hms_opt(0, 0, 0).expect("midnight"))
        .num_microseconds()
        .ok_or_else(|| format!("timestamp `{text}` is out of range"))
}

/// Formats microseconds since 2000-01-01 as PostgreSQL `timestamp` text.
///
/// # Errors
///
/// Returns an error for instants outside the range this conversion can represent.
pub fn format_timestamp_pg_micros(micros: i64) -> Result<String, String> {
    match micros {
        i64::MAX => return Ok(PG_INFINITY_TEXT.to_string()),
        i64::MIN => return Ok(PG_NEG_INFINITY_TEXT.to_string()),
        _ => {}
    }
    let timestamp = pg_epoch_date()
        .and_hms_opt(0, 0, 0)
        .expect("midnight")
        .checked_add_signed(Duration::microseconds(micros))
        .ok_or_else(|| format!("timestamp value {micros} is out of range"))?;
    let date = format_ymd(timestamp.date());
    let time = timestamp.time();
    let (date, bc) = match date.strip_suffix(" BC") {
        Some(rest) => (rest.to_string(), true),
        None => (date, false),
    };
    let micros_part = time.nanosecond() / 1_000;
    let mut out = format!(
        "{date}T{:02}:{:02}:{:02}",
        time.hour(),
        time.minute(),
        time.second()
    );
    if micros_part != 0 {
        // PostgreSQL prints the fraction without trailing zeros, and the mirror's tombstone keys
        // (`to_jsonb`) must compare equal to these cold keys.
        out.push_str(format!(".{micros_part:06}").trim_end_matches('0'));
    }
    if bc {
        out.push_str(" BC");
    }
    Ok(out)
}

/// Formats microseconds since 2000-01-01 as the text `to_jsonb(timestamptz)` produces when the session
/// time zone is UTC: ISO 8601 with a `+00:00` offset (before any ` BC` suffix), or `(-)infinity`.
///
/// # Errors
///
/// Returns an error for instants outside the range this conversion can represent.
pub fn format_timestamptz_json_utc(micros: i64) -> Result<String, String> {
    let text = format_timestamp_pg_micros(micros)?;
    if micros == i64::MAX || micros == i64::MIN {
        return Ok(text);
    }
    Ok(match text.strip_suffix(" BC") {
        Some(rest) => format!("{rest}+00:00 BC"),
        None => format!("{text}+00:00"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_round_trip_around_the_epoch_and_before() {
        for (text, days) in [
            ("2000-01-01", 0),
            ("2000-01-02", 1),
            ("1999-12-31", -1),
            ("2020-02-29", 7_364),
            ("1970-01-01", -10_957),
        ] {
            assert_eq!(parse_date_pg_days(text).unwrap(), days, "{text}");
            assert_eq!(format_date_pg_days(days).unwrap(), text, "{text}");
        }
    }

    #[test]
    fn bc_dates_follow_postgres_numbering() {
        // 1 BC is astronomical year 0 (a leap year), so 0001-02-29 BC exists.
        let days = parse_date_pg_days("0001-02-29 BC").unwrap();
        assert_eq!(format_date_pg_days(days).unwrap(), "0001-02-29 BC");
        let days = parse_date_pg_days("0044-03-15 BC").unwrap();
        assert!(days < parse_date_pg_days("0001-01-01").unwrap());
        assert_eq!(format_date_pg_days(days).unwrap(), "0044-03-15 BC");
    }

    #[test]
    fn infinity_is_the_extreme_value_in_both_directions() {
        assert_eq!(parse_date_pg_days("infinity").unwrap(), i32::MAX);
        assert_eq!(parse_date_pg_days("-infinity").unwrap(), i32::MIN);
        assert_eq!(format_date_pg_days(i32::MAX).unwrap(), "infinity");
        assert_eq!(parse_timestamp_pg_micros("infinity").unwrap(), i64::MAX);
        assert_eq!(parse_timestamp_pg_micros("-infinity").unwrap(), i64::MIN);
        assert_eq!(format_timestamp_pg_micros(i64::MIN).unwrap(), "-infinity");
    }

    #[test]
    fn timestamps_round_trip_with_and_without_fractions() {
        for (text, micros) in [
            ("2000-01-01T00:00:00", 0),
            ("2000-01-01T00:00:01", 1_000_000),
            ("2000-01-01T00:00:00.000001", 1),
            ("1999-12-31T23:59:59.999999", -1),
            ("2020-06-15T12:34:56.789012", 645_539_696_789_012),
        ] {
            assert_eq!(parse_timestamp_pg_micros(text).unwrap(), micros, "{text}");
            assert_eq!(format_timestamp_pg_micros(micros).unwrap(), text, "{text}");
        }
        // The space separator PostgreSQL prints by default parses too.
        assert_eq!(
            parse_timestamp_pg_micros("2000-01-01 00:00:01").unwrap(),
            1_000_000
        );
        assert_eq!(parse_timestamp_pg_micros("2000-01-02").unwrap(), 86_400_000_000);
    }

    #[test]
    fn bc_timestamps_round_trip() {
        let micros = parse_timestamp_pg_micros("0044-03-15T10:30:00.5 BC").unwrap();
        assert_eq!(
            format_timestamp_pg_micros(micros).unwrap(),
            "0044-03-15T10:30:00.5 BC"
        );
    }

    #[test]
    fn timestamptz_json_matches_postgres_in_utc() {
        assert_eq!(format_timestamptz_json_utc(0).unwrap(), "2000-01-01T00:00:00+00:00");
        assert_eq!(format_timestamptz_json_utc(500_000).unwrap(), "2000-01-01T00:00:00.5+00:00");
        assert_eq!(format_timestamptz_json_utc(i64::MAX).unwrap(), "infinity");
        assert_eq!(format_timestamptz_json_utc(i64::MIN).unwrap(), "-infinity");
        let bc = parse_timestamp_pg_micros("0044-03-15T12:00:00 BC").unwrap();
        assert_eq!(format_timestamptz_json_utc(bc).unwrap(), "0044-03-15T12:00:00+00:00 BC");
    }

    #[test]
    fn garbage_is_rejected_not_guessed() {
        assert!(parse_date_pg_days("not a date").is_err());
        assert!(parse_date_pg_days("2020-13-01").is_err());
        assert!(parse_timestamp_pg_micros("2020-01-01T25:00:00").is_err());
    }
}

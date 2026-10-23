use super::push_bytes;

pub const HTTP_DATE_BYTES: usize = 32;

// NOTE: IMF-fixdate math is Howard Hinnant's days-from-civil/civil-from-days pair, GMT only; parse also
// accepts the two obsolete RFC 9110 formats, splitting two-digit RFC 850 years 00-69/70-99 like RFC 6265.
pub fn write_http_date(secs: u64, out: &mut [u8]) -> usize {
    let days = secs / 86400;
    let clock = secs % 86400;
    let (year, month, day) = civil_from_days(days);
    let mut len = push_bytes(out, 0, SHORT_WEEKDAYS[((days + 4) % 7) as usize]);
    len = push_bytes(out, len, b", ");
    len = push_two(out, len, day);
    len = push_bytes(out, len, b" ");
    len = push_bytes(out, len, SHORT_MONTHS[month as usize - 1]);
    len = push_bytes(out, len, b" ");
    len = push_year(out, len, year);
    len = push_bytes(out, len, b" ");
    len = push_two(out, len, clock / 3600);
    len = push_bytes(out, len, b":");
    len = push_two(out, len, clock % 3600 / 60);
    len = push_bytes(out, len, b":");
    len = push_two(out, len, clock % 60);
    push_bytes(out, len, b" GMT")
}

pub fn parse_http_date(value: &[u8]) -> Option<u64> {
    if let Some(since) = parse_imf_date(value) {
        return Some(since);
    }
    if let Some(since) = parse_rfc850_date(value) {
        return Some(since);
    }
    parse_asctime_date(value)
}

pub(crate) fn system_time_secs(time: Option<std::time::SystemTime>) -> u64 {
    let span = time.and_then(|instant| instant.duration_since(std::time::UNIX_EPOCH).ok());
    span.map(|window| window.as_secs()).unwrap_or(0)
}

const SHORT_WEEKDAYS: [&[u8]; 7] = [b"Sun", b"Mon", b"Tue", b"Wed", b"Thu", b"Fri", b"Sat"];
const SHORT_MONTHS: [&[u8]; 12] = [
    b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov", b"Dec",
];
const FULL_WEEKDAYS: [&[u8]; 7] = [
    b"Sunday",
    b"Monday",
    b"Tuesday",
    b"Wednesday",
    b"Thursday",
    b"Friday",
    b"Saturday",
];

fn civil_from_days(days: u64) -> (u64, u64, u64) {
    let shifted = days + 719468;
    let era = shifted / 146097;
    let day_of_era = shifted - era * 146097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_part = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_part + 2) / 5 + 1;
    let month = if month_part < 10 {
        month_part + 3
    } else {
        month_part - 9
    };
    (year + u64::from(month <= 2), month, day)
}

fn days_from_civil(year: u64, month: u64, day: u64) -> Option<i64> {
    if year == 0 {
        return None;
    }
    let adjusted = year as i64 - i64::from(month <= 2);
    let era = adjusted / 400;
    let year_of_era = adjusted - era * 400;
    let month_part = month as i64 + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_part + 2) / 5 + day as i64 - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Some(era * 146097 + day_of_era - 719468)
}

fn days_in_month(year: u64, month: u64) -> u64 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        _ => 31,
    }
}

fn http_secs(year: u64, month: u64, day: u64, hour: u64, minute: u64, second: u64) -> Option<u64> {
    if month == 0 || month > 12 || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let days = days_from_civil(year, month, day)?;
    let total = days * 86400 + hour as i64 * 3600 + minute as i64 * 60 + second as i64;
    u64::try_from(total).ok()
}

fn parse_imf_date(value: &[u8]) -> Option<u64> {
    if value.len() != 29 {
        return None;
    }
    if value[3] != b',' || value[4] != b' ' || value[7] != b' ' || value[11] != b' ' {
        return None;
    }
    if value[16] != b' ' || value[19] != b':' || value[22] != b':' || value[25] != b' ' {
        return None;
    }
    if &value[26..29] != b"GMT" || !value[..3].iter().all(|byte| byte.is_ascii_alphabetic()) {
        return None;
    }
    let day = two_digits(value[5], value[6])?;
    let month = month_value(&value[8..11])?;
    let year = four_digits(&value[12..16])?;
    let hour = two_digits(value[17], value[18])?;
    let minute = two_digits(value[20], value[21])?;
    let second = two_digits(value[23], value[24])?;
    http_secs(year, month, day, hour, minute, second)
}

fn parse_rfc850_date(value: &[u8]) -> Option<u64> {
    let comma = value.iter().position(|byte| *byte == b',')?;
    if !FULL_WEEKDAYS.iter().any(|day| *day == &value[..comma]) {
        return None;
    }
    if value.get(comma + 1) != Some(&b' ') {
        return None;
    }
    let rest = &value[comma + 2..];
    if rest.len() != 22 || rest[2] != b'-' || rest[6] != b'-' || rest[9] != b' ' {
        return None;
    }
    if rest[12] != b':' || rest[15] != b':' || rest[18] != b' ' || &rest[19..22] != b"GMT" {
        return None;
    }
    let day = two_digits(rest[0], rest[1])?;
    let month = month_value(&rest[3..6])?;
    let short = two_digits(rest[7], rest[8])?;
    let year = if short >= 70 {
        1900 + short
    } else {
        2000 + short
    };
    let hour = two_digits(rest[10], rest[11])?;
    let minute = two_digits(rest[13], rest[14])?;
    let second = two_digits(rest[16], rest[17])?;
    http_secs(year, month, day, hour, minute, second)
}

fn parse_asctime_date(value: &[u8]) -> Option<u64> {
    if value.len() != 24 || value[3] != b' ' || value[7] != b' ' || value[10] != b' ' {
        return None;
    }
    if value[13] != b':' || value[16] != b':' || value[19] != b' ' {
        return None;
    }
    if !value[..3].iter().all(|byte| byte.is_ascii_alphabetic()) {
        return None;
    }
    let month = month_value(&value[4..7])?;
    let day = match (value[8], value[9]) {
        (b' ', ones) if ones.is_ascii_digit() => u64::from(ones - b'0'),
        (tens, ones) => two_digits(tens, ones)?,
    };
    let hour = two_digits(value[11], value[12])?;
    let minute = two_digits(value[14], value[15])?;
    let second = two_digits(value[17], value[18])?;
    let year = four_digits(&value[20..24])?;
    http_secs(year, month, day, hour, minute, second)
}

fn two_digits(high: u8, low: u8) -> Option<u64> {
    if !high.is_ascii_digit() || !low.is_ascii_digit() {
        return None;
    }
    Some(u64::from(high - b'0') * 10 + u64::from(low - b'0'))
}

fn four_digits(digits: &[u8]) -> Option<u64> {
    let mut value = 0u64;
    for byte in digits.iter() {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value * 10 + u64::from(byte - b'0');
    }
    Some(value)
}

fn month_value(name: &[u8]) -> Option<u64> {
    SHORT_MONTHS
        .iter()
        .position(|month| *month == name)
        .map(|index| index as u64 + 1)
}

fn push_two(out: &mut [u8], len: usize, value: u64) -> usize {
    out[len] = b'0' + (value / 10) as u8;
    out[len + 1] = b'0' + (value % 10) as u8;
    len + 2
}

fn push_year(out: &mut [u8], mut len: usize, value: u64) -> usize {
    let mut digits = [0u8; 20];
    let mut count = 0;
    let mut rest = value;
    loop {
        digits[count] = b'0' + (rest % 10) as u8;
        count += 1;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    for _ in count..count.max(4) {
        out[len] = b'0';
        len += 1;
    }
    while count > 0 {
        count -= 1;
        out[len] = digits[count];
        len += 1;
    }
    len
}

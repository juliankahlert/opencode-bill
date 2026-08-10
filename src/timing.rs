//! Millisecond arithmetic and human-readable time rendering.

use chrono::{DateTime, Local};

/// Timestamp format used for the bill's start and end lines.
pub(crate) const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S %:z";

/// Renders a Unix millisecond timestamp as local RFC3339, seconds precision.
///
/// The duration-style ISO form (e.g. `2026-08-10T00:47:03+02:00`) names when
/// a root session started; the picker menu and the session bill header row
/// show it.  A timestamp chrono cannot represent renders as `-`.
pub fn format_local_iso(created_ms: i64) -> String {
    DateTime::from_timestamp_millis(created_ms)
        .map(|instant| {
            instant
                .with_timezone(&Local)
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        })
        .unwrap_or_else(|| "-".to_owned())
}

/// The earlier of two optional millisecond timestamps.
pub(crate) fn earliest(first: Option<u64>, second: Option<u64>) -> Option<u64> {
    [first, second].into_iter().flatten().min()
}

/// The later of two optional millisecond timestamps.
pub(crate) fn latest(first: Option<u64>, second: Option<u64>) -> Option<u64> {
    [first, second].into_iter().flatten().max()
}

/// API time of one assistant message: its span minus the time spent in tools.
///
/// Tool spans overlap when calls run in parallel, so they are merged before
/// being subtracted. Spans are clamped to the message so a tool that outlives
/// its message cannot push the result below zero.
pub(crate) fn message_api_ms(
    created: Option<u64>,
    completed: Option<u64>,
    tools: &[(u64, u64)],
) -> u64 {
    let (start, end) = match (created, completed) {
        (Some(start), Some(end)) if end > start => (start, end),
        _ => return 0,
    };
    let mut spans = tools
        .iter()
        .map(|(open, close)| ((*open).clamp(start, end), (*close).clamp(start, end)))
        .filter(|(open, close)| close > open)
        .collect::<Vec<_>>();

    spans.sort_unstable();

    let (busy, current) = spans
        .into_iter()
        .fold((0u64, None), |(busy, current), (open, close)| {
            let (busy_added, next) = merge_tool_span(current, open, close);

            (busy + busy_added, next)
        });
    let busy = busy + current.map_or(0, |(open, close)| close - open);

    (end - start).saturating_sub(busy)
}

/// Merges one sorted tool span into the running merged span.
///
/// Returns the busy time the span adds plus the next running span. An
/// overlapping span extends the running one; a later span closes the run and
/// starts a new one.
fn merge_tool_span(
    current: Option<(u64, u64)>,
    open: u64,
    close: u64,
) -> (u64, Option<(u64, u64)>) {
    let Some((merged_open, merged_close)) = current else {
        return (0, Some((open, close)));
    };

    if open <= merged_close {
        (0, Some((merged_open, merged_close.max(close))))
    } else {
        (merged_close - merged_open, Some((open, close)))
    }
}

/// Millisecond span between two timestamps, zero when either is missing.
pub(crate) fn span_ms(start: Option<u64>, end: Option<u64>) -> u64 {
    match (start, end) {
        (Some(start), Some(end)) if end > start => end - start,
        _ => 0,
    }
}

/// Renders a Unix millisecond timestamp in the local time zone.
///
/// The UTC offset is always printed, so a bill stays readable when it is sent
/// to somebody in another zone. Systems without a usable zone fall back to
/// UTC, which prints as `+00:00`.
pub(crate) fn format_timestamp(millis: Option<u64>) -> String {
    millis
        .and_then(|value| i64::try_from(value).ok())
        .and_then(DateTime::from_timestamp_millis)
        .map_or_else(
            || "-".to_owned(),
            |instant| {
                instant
                    .with_timezone(&Local)
                    .format(TIMESTAMP_FORMAT)
                    .to_string()
            },
        )
}

/// Renders a millisecond duration as a compact human-readable string.
pub(crate) fn format_duration(millis: u64) -> String {
    let seconds = millis / 1000;

    if millis < 1000 {
        return format!("{millis}ms");
    }

    if seconds < 60 {
        return format!("{seconds}s");
    }

    let minutes = seconds / 60;

    if minutes < 60 {
        return format!("{minutes}m {}s", seconds % 60);
    }

    format!("{}h {}m", minutes / 60, minutes % 60)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::FixedOffset;

    #[test]
    fn formats_timestamps() {
        // The zone of the test machine is unknown, so the rendering itself is
        // checked against explicit offsets.
        let rendered = |millis, hours| {
            DateTime::from_timestamp_millis(millis)
                .unwrap()
                .with_timezone(&FixedOffset::east_opt(hours * 3600).unwrap())
                .format(TIMESTAMP_FORMAT)
                .to_string()
        };

        assert_eq!(format_timestamp(None), "-");
        assert_eq!(rendered(1_786_027_829_613, 0), "2026-08-06 14:50:29 +00:00");
        assert_eq!(rendered(1_786_027_829_613, 2), "2026-08-06 16:50:29 +02:00");
        // A local date one day behind UTC.
        assert_eq!(
            rendered(1_709_164_800_000, -5),
            "2024-02-28 19:00:00 -05:00"
        );
        assert!(format_timestamp(Some(1_786_027_829_613)).starts_with("2026-08-0"));
    }

    #[test]
    fn formats_durations() {
        assert_eq!(format_duration(0), "0ms");
        assert_eq!(format_duration(999), "999ms");
        assert_eq!(format_duration(3_200), "3s");
        assert_eq!(format_duration(65_000), "1m 5s");
        assert_eq!(format_duration(3_661_000), "1h 1m");
    }

    #[test]
    fn formats_local_iso_seconds_precision() {
        // RFC3339 seconds with an offset is always 25 chars, whatever the
        // test machine's zone: YYYY-MM-DDTHH:MM:SS+HH:MM.
        let rendered = format_local_iso(1_786_315_623_640);

        assert_eq!(rendered.len(), 25, "rendered: {rendered}");
        assert!(rendered.contains('T'), "rendered: {rendered}");
        assert_eq!(format_local_iso(i64::MIN), "-");
    }

    #[test]
    fn caps_tool_time_at_the_message_span() {
        // A tool that outlives its message cannot drive API time negative.
        assert_eq!(message_api_ms(Some(0), Some(1000), &[(500, 9000)]), 500);
        assert_eq!(message_api_ms(Some(0), Some(1000), &[(0, 9000)]), 0);
        // An unfinished message has no measurable API time.
        assert_eq!(message_api_ms(Some(0), None, &[]), 0);
    }
}

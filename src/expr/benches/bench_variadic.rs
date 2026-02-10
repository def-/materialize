// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bencher::{Bencher, benchmark_group, benchmark_main};
use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use mz_expr::{MirScalarExpr, VariadicFunc};
use mz_repr::adt::date::Date;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, RowArena, SqlScalarType};

fn lit(datum: Datum, typ: SqlScalarType) -> MirScalarExpr {
    MirScalarExpr::literal_ok(datum, typ)
}

// ---------------------------------------------------------------------------
// Macro for generating variadic benchmarks
// ---------------------------------------------------------------------------

macro_rules! bench_variadic {
    ($bench_name:ident, $variant:expr, $( ($datum:expr, $typ:expr) ),+ $(,)? ) => {
        fn $bench_name(b: &mut Bencher) {
            let func = $variant;
            let exprs = vec![ $( lit($datum, $typ) ),+ ];
            let arena = RowArena::new();
            let datums: &[Datum] = &[];
            b.iter(|| func.eval(datums, &arena, &exprs));
        }
    };
}

// ===========================================================================
// Logic / comparison
// ===========================================================================

bench_variadic!(
    bench_coalesce_happy,
    VariadicFunc::Coalesce,
    (Datum::Int32(1), SqlScalarType::Int32),
    (Datum::Int32(2), SqlScalarType::Int32)
);

bench_variadic!(
    bench_greatest_happy,
    VariadicFunc::Greatest,
    (Datum::Int32(10), SqlScalarType::Int32),
    (Datum::Int32(20), SqlScalarType::Int32),
    (Datum::Int32(5), SqlScalarType::Int32)
);

bench_variadic!(
    bench_least_happy,
    VariadicFunc::Least,
    (Datum::Int32(10), SqlScalarType::Int32),
    (Datum::Int32(20), SqlScalarType::Int32),
    (Datum::Int32(5), SqlScalarType::Int32)
);

bench_variadic!(
    bench_and_happy,
    VariadicFunc::And,
    (Datum::True, SqlScalarType::Bool),
    (Datum::True, SqlScalarType::Bool),
    (Datum::True, SqlScalarType::Bool)
);

bench_variadic!(
    bench_or_happy,
    VariadicFunc::Or,
    (Datum::False, SqlScalarType::Bool),
    (Datum::False, SqlScalarType::Bool),
    (Datum::True, SqlScalarType::Bool)
);

bench_variadic!(
    bench_error_if_null_happy,
    VariadicFunc::ErrorIfNull,
    (Datum::Int32(42), SqlScalarType::Int32),
    (Datum::String("value was null"), SqlScalarType::String)
);

// ===========================================================================
// String functions
// ===========================================================================

bench_variadic!(
    bench_concat_happy,
    VariadicFunc::Concat,
    (Datum::String("hello"), SqlScalarType::String),
    (Datum::String(" "), SqlScalarType::String),
    (Datum::String("world"), SqlScalarType::String)
);

bench_variadic!(
    bench_concat_ws_happy,
    VariadicFunc::ConcatWs,
    (Datum::String(", "), SqlScalarType::String),
    (Datum::String("one"), SqlScalarType::String),
    (Datum::String("two"), SqlScalarType::String),
    (Datum::String("three"), SqlScalarType::String)
);

bench_variadic!(
    bench_substr_happy,
    VariadicFunc::Substr,
    (Datum::String("hello world"), SqlScalarType::String),
    (Datum::Int32(7), SqlScalarType::Int32),
    (Datum::Int32(5), SqlScalarType::Int32)
);

bench_variadic!(
    bench_replace_happy,
    VariadicFunc::Replace,
    (Datum::String("hello world"), SqlScalarType::String),
    (Datum::String("world"), SqlScalarType::String),
    (Datum::String("rust"), SqlScalarType::String)
);

bench_variadic!(
    bench_translate_happy,
    VariadicFunc::Translate,
    (Datum::String("hello"), SqlScalarType::String),
    (Datum::String("helo"), SqlScalarType::String),
    (Datum::String("HELO"), SqlScalarType::String)
);

bench_variadic!(
    bench_split_part_happy,
    VariadicFunc::SplitPart,
    (Datum::String("one.two.three"), SqlScalarType::String),
    (Datum::String("."), SqlScalarType::String),
    (Datum::Int32(2), SqlScalarType::Int32)
);

bench_variadic!(
    bench_pad_leading_happy,
    VariadicFunc::PadLeading,
    (Datum::String("hi"), SqlScalarType::String),
    (Datum::Int32(10), SqlScalarType::Int32),
    (Datum::String("*"), SqlScalarType::String)
);

bench_variadic!(
    bench_regexp_match_happy,
    VariadicFunc::RegexpMatch,
    (Datum::String("hello world 42"), SqlScalarType::String),
    (Datum::String("(\\d+)"), SqlScalarType::String)
);

bench_variadic!(
    bench_regexp_split_to_array_happy,
    VariadicFunc::RegexpSplitToArray,
    (Datum::String("one1two2three"), SqlScalarType::String),
    (Datum::String("\\d"), SqlScalarType::String)
);

bench_variadic!(
    bench_regexp_replace_happy,
    VariadicFunc::RegexpReplace,
    (Datum::String("hello world"), SqlScalarType::String),
    (Datum::String("world"), SqlScalarType::String),
    (Datum::String("rust"), SqlScalarType::String)
);

bench_variadic!(
    bench_string_to_array_happy,
    VariadicFunc::StringToArray,
    (Datum::String("one,two,three"), SqlScalarType::String),
    (Datum::String(","), SqlScalarType::String)
);

// ===========================================================================
// JSON functions
// ===========================================================================

bench_variadic!(
    bench_jsonb_build_array_happy,
    VariadicFunc::JsonbBuildArray,
    (Datum::Int32(1), SqlScalarType::Jsonb),
    (Datum::String("two"), SqlScalarType::Jsonb),
    (Datum::True, SqlScalarType::Jsonb)
);

bench_variadic!(
    bench_jsonb_build_object_happy,
    VariadicFunc::JsonbBuildObject,
    (Datum::String("key1"), SqlScalarType::Jsonb),
    (Datum::Int32(1), SqlScalarType::Jsonb),
    (Datum::String("key2"), SqlScalarType::Jsonb),
    (Datum::String("value2"), SqlScalarType::Jsonb)
);

// ===========================================================================
// HMAC / crypto
// ===========================================================================

bench_variadic!(
    bench_hmac_string_happy,
    VariadicFunc::HmacString,
    (Datum::String("hello"), SqlScalarType::String),
    (Datum::String("secret"), SqlScalarType::String),
    (Datum::String("sha256"), SqlScalarType::String)
);

bench_variadic!(
    bench_hmac_bytes_happy,
    VariadicFunc::HmacBytes,
    (Datum::Bytes(&[1, 2, 3, 4]), SqlScalarType::Bytes),
    (Datum::Bytes(&[5, 6, 7, 8]), SqlScalarType::Bytes),
    (Datum::String("sha256"), SqlScalarType::String)
);

// ===========================================================================
// Timestamp / date functions (hand-written due to complex datum construction)
// ===========================================================================

fn bench_make_timestamp_happy(b: &mut Bencher) {
    let func = VariadicFunc::MakeTimestamp;
    let exprs = vec![
        lit(Datum::Int64(2024), SqlScalarType::Int64),
        lit(Datum::Int64(1), SqlScalarType::Int64),
        lit(Datum::Int64(15), SqlScalarType::Int64),
        lit(Datum::Int64(12), SqlScalarType::Int64),
        lit(Datum::Int64(30), SqlScalarType::Int64),
        lit(
            Datum::Float64(ordered_float::OrderedFloat(45.0)),
            SqlScalarType::Float64,
        ),
    ];
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_date_bin_timestamp_happy(b: &mut Bencher) {
    let func = VariadicFunc::DateBinTimestamp;
    let interval = Interval::new(0, 0, 3_600_000_000); // 1 hour
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap(),
    )
    .unwrap();
    let origin = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let exprs = vec![
        lit(Datum::Interval(interval), SqlScalarType::Interval),
        lit(
            Datum::Timestamp(ts),
            SqlScalarType::Timestamp { precision: None },
        ),
        lit(
            Datum::Timestamp(origin),
            SqlScalarType::Timestamp { precision: None },
        ),
    ];
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_date_bin_timestamp_tz_happy(b: &mut Bencher) {
    let func = VariadicFunc::DateBinTimestampTz;
    let interval = Interval::new(0, 0, 3_600_000_000); // 1 hour
    let ts = CheckedTimestamp::from_timestamplike(
        Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap(),
    )
    .unwrap();
    let origin =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap())
            .unwrap();
    let exprs = vec![
        lit(Datum::Interval(interval), SqlScalarType::Interval),
        lit(
            Datum::TimestampTz(ts),
            SqlScalarType::TimestampTz { precision: None },
        ),
        lit(
            Datum::TimestampTz(origin),
            SqlScalarType::TimestampTz { precision: None },
        ),
    ];
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_date_diff_timestamp_happy(b: &mut Bencher) {
    let func = VariadicFunc::DateDiffTimestamp;
    let ts1 = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap(),
    )
    .unwrap();
    let ts2 = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 6, 20)
            .unwrap()
            .and_hms_opt(8, 15, 30)
            .unwrap(),
    )
    .unwrap();
    let exprs = vec![
        lit(Datum::String("day"), SqlScalarType::String),
        lit(
            Datum::Timestamp(ts1),
            SqlScalarType::Timestamp { precision: None },
        ),
        lit(
            Datum::Timestamp(ts2),
            SqlScalarType::Timestamp { precision: None },
        ),
    ];
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_date_diff_timestamp_tz_happy(b: &mut Bencher) {
    let func = VariadicFunc::DateDiffTimestampTz;
    let ts1 = CheckedTimestamp::from_timestamplike(
        Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap(),
    )
    .unwrap();
    let ts2 =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 6, 20, 8, 15, 30).unwrap())
            .unwrap();
    let exprs = vec![
        lit(Datum::String("day"), SqlScalarType::String),
        lit(
            Datum::TimestampTz(ts1),
            SqlScalarType::TimestampTz { precision: None },
        ),
        lit(
            Datum::TimestampTz(ts2),
            SqlScalarType::TimestampTz { precision: None },
        ),
    ];
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_date_diff_date_happy(b: &mut Bencher) {
    let func = VariadicFunc::DateDiffDate;
    let d1 = Date::from_pg_epoch(8_415).unwrap(); // ~2024-01-15
    let d2 = Date::from_pg_epoch(8_572).unwrap(); // ~2024-06-20
    let exprs = vec![
        lit(Datum::String("day"), SqlScalarType::String),
        lit(Datum::Date(d1), SqlScalarType::Date),
        lit(Datum::Date(d2), SqlScalarType::Date),
    ];
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_date_diff_time_happy(b: &mut Bencher) {
    let func = VariadicFunc::DateDiffTime;
    let t1 = NaiveTime::from_hms_opt(12, 30, 0).unwrap();
    let t2 = NaiveTime::from_hms_opt(18, 45, 30).unwrap();
    let exprs = vec![
        lit(Datum::String("hour"), SqlScalarType::String),
        lit(Datum::Time(t1), SqlScalarType::Time),
        lit(Datum::Time(t2), SqlScalarType::Time),
    ];
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_timezone_time_happy(b: &mut Bencher) {
    let func = VariadicFunc::TimezoneTime;
    let t = NaiveTime::from_hms_opt(12, 30, 0).unwrap();
    let wall =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let exprs = vec![
        lit(Datum::String("UTC"), SqlScalarType::String),
        lit(Datum::Time(t), SqlScalarType::Time),
        lit(
            Datum::TimestampTz(wall),
            SqlScalarType::TimestampTz { precision: None },
        ),
    ];
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| func.eval(datums, &arena, &exprs));
}

// ===========================================================================
// Benchmark groups
// ===========================================================================

benchmark_group!(
    logic_string_benches,
    bench_coalesce_happy,
    bench_greatest_happy,
    bench_least_happy,
    bench_and_happy,
    bench_or_happy,
    bench_error_if_null_happy,
    bench_concat_happy,
    bench_concat_ws_happy,
    bench_substr_happy,
    bench_replace_happy,
    bench_translate_happy,
    bench_split_part_happy,
    bench_pad_leading_happy,
    bench_regexp_match_happy,
    bench_regexp_split_to_array_happy,
    bench_regexp_replace_happy,
    bench_string_to_array_happy
);

benchmark_group!(
    json_crypto_benches,
    bench_jsonb_build_array_happy,
    bench_jsonb_build_object_happy,
    bench_hmac_string_happy,
    bench_hmac_bytes_happy
);

benchmark_group!(
    timestamp_benches,
    bench_make_timestamp_happy,
    bench_date_bin_timestamp_happy,
    bench_date_bin_timestamp_tz_happy,
    bench_date_diff_timestamp_happy,
    bench_date_diff_timestamp_tz_happy,
    bench_date_diff_date_happy,
    bench_date_diff_time_happy,
    bench_timezone_time_happy
);

benchmark_main!(logic_string_benches, json_crypto_benches, timestamp_benches);

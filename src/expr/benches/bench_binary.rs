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
use dec::OrderedDecimal;
use mz_expr::{BinaryFunc, MirScalarExpr, func};
use mz_repr::adt::date::Date;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::numeric::Numeric;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, RowArena, SqlScalarType};
use ordered_float::OrderedFloat;

fn lit(datum: Datum, typ: SqlScalarType) -> MirScalarExpr {
    MirScalarExpr::literal_ok(datum, typ)
}

// ---------------------------------------------------------------------------
// Macro for generating binary benchmarks
// ---------------------------------------------------------------------------

macro_rules! bench_binary {
    ($bench_name:ident, $variant:ident, $struct_expr:expr,
     $a_datum:expr, $a_type:expr, $b_datum:expr, $b_type:expr) => {
        fn $bench_name(b: &mut Bencher) {
            let f = BinaryFunc::$variant($struct_expr);
            let a = lit($a_datum, $a_type);
            let e = lit($b_datum, $b_type);
            let arena = RowArena::new();
            let datums: &[Datum] = &[];
            b.iter(|| f.eval(datums, &arena, &a, &e));
        }
    };
}

/// Like bench_binary! but evaluates multiple input pairs per iteration.
macro_rules! bench_binary_multi {
    ($bench_name:ident, $variant:ident, $struct_expr:expr,
     $a_type:expr, $b_type:expr,
     [ $( ($a_datum:expr, $b_datum:expr) ),+ $(,)? ]) => {
        fn $bench_name(b: &mut Bencher) {
            let f = BinaryFunc::$variant($struct_expr);
            let inputs: Vec<(MirScalarExpr, MirScalarExpr)> = vec![
                $( (lit($a_datum, $a_type), lit($b_datum, $b_type)) ),+
            ];
            let arena = RowArena::new();
            let datums: &[Datum] = &[];
            b.iter(|| {
                for (a, e) in &inputs {
                    let _ = f.eval(datums, &arena, a, e);
                }
            });
        }
    };
}

// ===========================================================================
// Step 3: Arithmetic functions
// ===========================================================================

// --- Add ---
bench_binary!(
    bench_add_int16_happy,
    AddInt16,
    func::AddInt16,
    Datum::Int16(1),
    SqlScalarType::Int16,
    Datum::Int16(2),
    SqlScalarType::Int16
);
bench_binary!(
    bench_add_int16_error,
    AddInt16,
    func::AddInt16,
    Datum::Int16(i16::MAX),
    SqlScalarType::Int16,
    Datum::Int16(1),
    SqlScalarType::Int16
);
bench_binary!(
    bench_add_int32_happy,
    AddInt32,
    func::AddInt32,
    Datum::Int32(1),
    SqlScalarType::Int32,
    Datum::Int32(2),
    SqlScalarType::Int32
);
bench_binary!(
    bench_add_int32_error,
    AddInt32,
    func::AddInt32,
    Datum::Int32(i32::MAX),
    SqlScalarType::Int32,
    Datum::Int32(1),
    SqlScalarType::Int32
);
bench_binary!(
    bench_add_int64_happy,
    AddInt64,
    func::AddInt64,
    Datum::Int64(1),
    SqlScalarType::Int64,
    Datum::Int64(2),
    SqlScalarType::Int64
);
bench_binary!(
    bench_add_int64_error,
    AddInt64,
    func::AddInt64,
    Datum::Int64(i64::MAX),
    SqlScalarType::Int64,
    Datum::Int64(1),
    SqlScalarType::Int64
);
bench_binary!(
    bench_add_uint16_happy,
    AddUint16,
    func::AddUint16,
    Datum::UInt16(1),
    SqlScalarType::UInt16,
    Datum::UInt16(2),
    SqlScalarType::UInt16
);
bench_binary!(
    bench_add_uint16_error,
    AddUint16,
    func::AddUint16,
    Datum::UInt16(u16::MAX),
    SqlScalarType::UInt16,
    Datum::UInt16(1),
    SqlScalarType::UInt16
);
bench_binary!(
    bench_add_uint32_happy,
    AddUint32,
    func::AddUint32,
    Datum::UInt32(1),
    SqlScalarType::UInt32,
    Datum::UInt32(2),
    SqlScalarType::UInt32
);
bench_binary!(
    bench_add_uint32_error,
    AddUint32,
    func::AddUint32,
    Datum::UInt32(u32::MAX),
    SqlScalarType::UInt32,
    Datum::UInt32(1),
    SqlScalarType::UInt32
);
bench_binary!(
    bench_add_uint64_happy,
    AddUint64,
    func::AddUint64,
    Datum::UInt64(1),
    SqlScalarType::UInt64,
    Datum::UInt64(2),
    SqlScalarType::UInt64
);
bench_binary!(
    bench_add_uint64_error,
    AddUint64,
    func::AddUint64,
    Datum::UInt64(u64::MAX),
    SqlScalarType::UInt64,
    Datum::UInt64(1),
    SqlScalarType::UInt64
);
bench_binary!(
    bench_add_float32_happy,
    AddFloat32,
    func::AddFloat32,
    Datum::Float32(OrderedFloat(1.0)),
    SqlScalarType::Float32,
    Datum::Float32(OrderedFloat(2.0)),
    SqlScalarType::Float32
);
bench_binary!(
    bench_add_float32_error,
    AddFloat32,
    func::AddFloat32,
    Datum::Float32(OrderedFloat(f32::MAX)),
    SqlScalarType::Float32,
    Datum::Float32(OrderedFloat(f32::MAX)),
    SqlScalarType::Float32
);
bench_binary!(
    bench_add_float64_happy,
    AddFloat64,
    func::AddFloat64,
    Datum::Float64(OrderedFloat(1.0)),
    SqlScalarType::Float64,
    Datum::Float64(OrderedFloat(2.0)),
    SqlScalarType::Float64
);
bench_binary!(
    bench_add_float64_error,
    AddFloat64,
    func::AddFloat64,
    Datum::Float64(OrderedFloat(f64::MAX)),
    SqlScalarType::Float64,
    Datum::Float64(OrderedFloat(f64::MAX)),
    SqlScalarType::Float64
);
bench_binary!(
    bench_add_numeric_happy,
    AddNumeric,
    func::AddNumeric,
    Datum::Numeric(OrderedDecimal(Numeric::from(1))),
    SqlScalarType::Numeric { max_scale: None },
    Datum::Numeric(OrderedDecimal(Numeric::from(2))),
    SqlScalarType::Numeric { max_scale: None }
);
bench_binary!(
    bench_add_interval_happy,
    AddInterval,
    func::AddInterval,
    Datum::Interval(Interval::new(1, 2, 3_000_000)),
    SqlScalarType::Interval,
    Datum::Interval(Interval::new(0, 1, 1_000_000)),
    SqlScalarType::Interval
);

fn bench_add_timestamp_interval_happy(b: &mut Bencher) {
    let f = BinaryFunc::AddTimestampInterval(func::AddTimestampInterval);
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let a = lit(
        Datum::Timestamp(ts),
        SqlScalarType::Timestamp { precision: None },
    );
    let e = lit(
        Datum::Interval(Interval::new(0, 1, 0)),
        SqlScalarType::Interval,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_add_timestamp_tz_interval_happy(b: &mut Bencher) {
    let f = BinaryFunc::AddTimestampTzInterval(func::AddTimestampTzInterval);
    let ts =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let a = lit(
        Datum::TimestampTz(ts),
        SqlScalarType::TimestampTz { precision: None },
    );
    let e = lit(
        Datum::Interval(Interval::new(0, 1, 0)),
        SqlScalarType::Interval,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_add_date_interval_happy(b: &mut Bencher) {
    let f = BinaryFunc::AddDateInterval(func::AddDateInterval);
    let a = lit(
        Datum::Date(Date::from_pg_epoch(0).unwrap()),
        SqlScalarType::Date,
    );
    let e = lit(
        Datum::Interval(Interval::new(1, 0, 0)),
        SqlScalarType::Interval,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_add_date_time_happy(b: &mut Bencher) {
    let f = BinaryFunc::AddDateTime(func::AddDateTime);
    let a = lit(
        Datum::Date(Date::from_pg_epoch(0).unwrap()),
        SqlScalarType::Date,
    );
    let e = lit(
        Datum::Time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
        SqlScalarType::Time,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_add_time_interval_happy(b: &mut Bencher) {
    let f = BinaryFunc::AddTimeInterval(func::AddTimeInterval);
    let a = lit(
        Datum::Time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
        SqlScalarType::Time,
    );
    let e = lit(
        Datum::Interval(Interval::new(0, 0, 3_600_000_000)),
        SqlScalarType::Interval,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// --- Sub ---
bench_binary!(
    bench_sub_int16_happy,
    SubInt16,
    func::SubInt16,
    Datum::Int16(10),
    SqlScalarType::Int16,
    Datum::Int16(3),
    SqlScalarType::Int16
);
bench_binary!(
    bench_sub_int16_error,
    SubInt16,
    func::SubInt16,
    Datum::Int16(i16::MIN),
    SqlScalarType::Int16,
    Datum::Int16(1),
    SqlScalarType::Int16
);
bench_binary!(
    bench_sub_int32_happy,
    SubInt32,
    func::SubInt32,
    Datum::Int32(10),
    SqlScalarType::Int32,
    Datum::Int32(3),
    SqlScalarType::Int32
);
bench_binary!(
    bench_sub_int32_error,
    SubInt32,
    func::SubInt32,
    Datum::Int32(i32::MIN),
    SqlScalarType::Int32,
    Datum::Int32(1),
    SqlScalarType::Int32
);
bench_binary!(
    bench_sub_int64_happy,
    SubInt64,
    func::SubInt64,
    Datum::Int64(10),
    SqlScalarType::Int64,
    Datum::Int64(3),
    SqlScalarType::Int64
);
bench_binary!(
    bench_sub_int64_error,
    SubInt64,
    func::SubInt64,
    Datum::Int64(i64::MIN),
    SqlScalarType::Int64,
    Datum::Int64(1),
    SqlScalarType::Int64
);
bench_binary!(
    bench_sub_uint16_happy,
    SubUint16,
    func::SubUint16,
    Datum::UInt16(10),
    SqlScalarType::UInt16,
    Datum::UInt16(3),
    SqlScalarType::UInt16
);
bench_binary!(
    bench_sub_uint16_error,
    SubUint16,
    func::SubUint16,
    Datum::UInt16(0),
    SqlScalarType::UInt16,
    Datum::UInt16(1),
    SqlScalarType::UInt16
);
bench_binary!(
    bench_sub_uint32_happy,
    SubUint32,
    func::SubUint32,
    Datum::UInt32(10),
    SqlScalarType::UInt32,
    Datum::UInt32(3),
    SqlScalarType::UInt32
);
bench_binary!(
    bench_sub_uint32_error,
    SubUint32,
    func::SubUint32,
    Datum::UInt32(0),
    SqlScalarType::UInt32,
    Datum::UInt32(1),
    SqlScalarType::UInt32
);
bench_binary!(
    bench_sub_uint64_happy,
    SubUint64,
    func::SubUint64,
    Datum::UInt64(10),
    SqlScalarType::UInt64,
    Datum::UInt64(3),
    SqlScalarType::UInt64
);
bench_binary!(
    bench_sub_uint64_error,
    SubUint64,
    func::SubUint64,
    Datum::UInt64(0),
    SqlScalarType::UInt64,
    Datum::UInt64(1),
    SqlScalarType::UInt64
);
bench_binary!(
    bench_sub_float32_happy,
    SubFloat32,
    func::SubFloat32,
    Datum::Float32(OrderedFloat(10.0)),
    SqlScalarType::Float32,
    Datum::Float32(OrderedFloat(3.0)),
    SqlScalarType::Float32
);
bench_binary!(
    bench_sub_float64_happy,
    SubFloat64,
    func::SubFloat64,
    Datum::Float64(OrderedFloat(10.0)),
    SqlScalarType::Float64,
    Datum::Float64(OrderedFloat(3.0)),
    SqlScalarType::Float64
);
bench_binary!(
    bench_sub_numeric_happy,
    SubNumeric,
    func::SubNumeric,
    Datum::Numeric(OrderedDecimal(Numeric::from(10))),
    SqlScalarType::Numeric { max_scale: None },
    Datum::Numeric(OrderedDecimal(Numeric::from(3))),
    SqlScalarType::Numeric { max_scale: None }
);
bench_binary!(
    bench_sub_interval_happy,
    SubInterval,
    func::SubInterval,
    Datum::Interval(Interval::new(1, 2, 3_000_000)),
    SqlScalarType::Interval,
    Datum::Interval(Interval::new(0, 1, 1_000_000)),
    SqlScalarType::Interval
);

fn bench_sub_timestamp_happy(b: &mut Bencher) {
    let f = BinaryFunc::SubTimestamp(func::SubTimestamp);
    let ts1 = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let ts2 = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let a = lit(
        Datum::Timestamp(ts1),
        SqlScalarType::Timestamp { precision: None },
    );
    let e = lit(
        Datum::Timestamp(ts2),
        SqlScalarType::Timestamp { precision: None },
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_sub_timestamp_tz_happy(b: &mut Bencher) {
    let f = BinaryFunc::SubTimestampTz(func::SubTimestampTz);
    let ts1 =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap())
            .unwrap();
    let ts2 =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let a = lit(
        Datum::TimestampTz(ts1),
        SqlScalarType::TimestampTz { precision: None },
    );
    let e = lit(
        Datum::TimestampTz(ts2),
        SqlScalarType::TimestampTz { precision: None },
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_sub_timestamp_interval_happy(b: &mut Bencher) {
    let f = BinaryFunc::SubTimestampInterval(func::SubTimestampInterval);
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let a = lit(
        Datum::Timestamp(ts),
        SqlScalarType::Timestamp { precision: None },
    );
    let e = lit(
        Datum::Interval(Interval::new(0, 1, 0)),
        SqlScalarType::Interval,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_sub_timestamp_tz_interval_happy(b: &mut Bencher) {
    let f = BinaryFunc::SubTimestampTzInterval(func::SubTimestampTzInterval);
    let ts =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap())
            .unwrap();
    let a = lit(
        Datum::TimestampTz(ts),
        SqlScalarType::TimestampTz { precision: None },
    );
    let e = lit(
        Datum::Interval(Interval::new(0, 1, 0)),
        SqlScalarType::Interval,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

bench_binary!(
    bench_sub_date_happy,
    SubDate,
    func::SubDate,
    Datum::Date(Date::from_pg_epoch(100).unwrap()),
    SqlScalarType::Date,
    Datum::Date(Date::from_pg_epoch(0).unwrap()),
    SqlScalarType::Date
);

fn bench_sub_date_interval_happy(b: &mut Bencher) {
    let f = BinaryFunc::SubDateInterval(func::SubDateInterval);
    let a = lit(
        Datum::Date(Date::from_pg_epoch(100).unwrap()),
        SqlScalarType::Date,
    );
    let e = lit(
        Datum::Interval(Interval::new(1, 0, 0)),
        SqlScalarType::Interval,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_sub_time_happy(b: &mut Bencher) {
    let f = BinaryFunc::SubTime(func::SubTime);
    let a = lit(
        Datum::Time(NaiveTime::from_hms_opt(14, 0, 0).unwrap()),
        SqlScalarType::Time,
    );
    let e = lit(
        Datum::Time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
        SqlScalarType::Time,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_sub_time_interval_happy(b: &mut Bencher) {
    let f = BinaryFunc::SubTimeInterval(func::SubTimeInterval);
    let a = lit(
        Datum::Time(NaiveTime::from_hms_opt(14, 0, 0).unwrap()),
        SqlScalarType::Time,
    );
    let e = lit(
        Datum::Interval(Interval::new(0, 0, 3_600_000_000)),
        SqlScalarType::Interval,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// --- Mul ---
bench_binary!(
    bench_mul_int16_happy,
    MulInt16,
    func::MulInt16,
    Datum::Int16(3),
    SqlScalarType::Int16,
    Datum::Int16(4),
    SqlScalarType::Int16
);
bench_binary!(
    bench_mul_int16_error,
    MulInt16,
    func::MulInt16,
    Datum::Int16(i16::MAX),
    SqlScalarType::Int16,
    Datum::Int16(2),
    SqlScalarType::Int16
);
bench_binary!(
    bench_mul_int32_happy,
    MulInt32,
    func::MulInt32,
    Datum::Int32(3),
    SqlScalarType::Int32,
    Datum::Int32(4),
    SqlScalarType::Int32
);
bench_binary!(
    bench_mul_int32_error,
    MulInt32,
    func::MulInt32,
    Datum::Int32(i32::MAX),
    SqlScalarType::Int32,
    Datum::Int32(2),
    SqlScalarType::Int32
);
bench_binary!(
    bench_mul_int64_happy,
    MulInt64,
    func::MulInt64,
    Datum::Int64(3),
    SqlScalarType::Int64,
    Datum::Int64(4),
    SqlScalarType::Int64
);
bench_binary!(
    bench_mul_int64_error,
    MulInt64,
    func::MulInt64,
    Datum::Int64(i64::MAX),
    SqlScalarType::Int64,
    Datum::Int64(2),
    SqlScalarType::Int64
);
bench_binary!(
    bench_mul_uint16_happy,
    MulUint16,
    func::MulUint16,
    Datum::UInt16(3),
    SqlScalarType::UInt16,
    Datum::UInt16(4),
    SqlScalarType::UInt16
);
bench_binary!(
    bench_mul_uint16_error,
    MulUint16,
    func::MulUint16,
    Datum::UInt16(u16::MAX),
    SqlScalarType::UInt16,
    Datum::UInt16(2),
    SqlScalarType::UInt16
);
bench_binary!(
    bench_mul_uint32_happy,
    MulUint32,
    func::MulUint32,
    Datum::UInt32(3),
    SqlScalarType::UInt32,
    Datum::UInt32(4),
    SqlScalarType::UInt32
);
bench_binary!(
    bench_mul_uint32_error,
    MulUint32,
    func::MulUint32,
    Datum::UInt32(u32::MAX),
    SqlScalarType::UInt32,
    Datum::UInt32(2),
    SqlScalarType::UInt32
);
bench_binary!(
    bench_mul_uint64_happy,
    MulUint64,
    func::MulUint64,
    Datum::UInt64(3),
    SqlScalarType::UInt64,
    Datum::UInt64(4),
    SqlScalarType::UInt64
);
bench_binary!(
    bench_mul_uint64_error,
    MulUint64,
    func::MulUint64,
    Datum::UInt64(u64::MAX),
    SqlScalarType::UInt64,
    Datum::UInt64(2),
    SqlScalarType::UInt64
);
bench_binary!(
    bench_mul_float32_happy,
    MulFloat32,
    func::MulFloat32,
    Datum::Float32(OrderedFloat(3.0)),
    SqlScalarType::Float32,
    Datum::Float32(OrderedFloat(4.0)),
    SqlScalarType::Float32
);
bench_binary!(
    bench_mul_float32_error,
    MulFloat32,
    func::MulFloat32,
    Datum::Float32(OrderedFloat(f32::MAX)),
    SqlScalarType::Float32,
    Datum::Float32(OrderedFloat(2.0)),
    SqlScalarType::Float32
);
bench_binary!(
    bench_mul_float64_happy,
    MulFloat64,
    func::MulFloat64,
    Datum::Float64(OrderedFloat(3.0)),
    SqlScalarType::Float64,
    Datum::Float64(OrderedFloat(4.0)),
    SqlScalarType::Float64
);
bench_binary!(
    bench_mul_float64_error,
    MulFloat64,
    func::MulFloat64,
    Datum::Float64(OrderedFloat(f64::MAX)),
    SqlScalarType::Float64,
    Datum::Float64(OrderedFloat(2.0)),
    SqlScalarType::Float64
);
bench_binary!(
    bench_mul_numeric_happy,
    MulNumeric,
    func::MulNumeric,
    Datum::Numeric(OrderedDecimal(Numeric::from(3))),
    SqlScalarType::Numeric { max_scale: None },
    Datum::Numeric(OrderedDecimal(Numeric::from(4))),
    SqlScalarType::Numeric { max_scale: None }
);
bench_binary!(
    bench_mul_interval_happy,
    MulInterval,
    func::MulInterval,
    Datum::Interval(Interval::new(1, 0, 0)),
    SqlScalarType::Interval,
    Datum::Float64(OrderedFloat(2.0)),
    SqlScalarType::Float64
);

// --- Div ---
bench_binary_multi!(
    bench_div_int16_happy,
    DivInt16,
    func::DivInt16,
    SqlScalarType::Int16,
    SqlScalarType::Int16,
    [
        (Datum::Int16(10), Datum::Int16(3)),
        (Datum::Int16(100), Datum::Int16(7)),
        (Datum::Int16(-50), Datum::Int16(3)),
        (Datum::Int16(i16::MAX), Datum::Int16(13)),
        (Datum::Int16(1), Datum::Int16(1)),
        (Datum::Int16(0), Datum::Int16(42)),
        (Datum::Int16(32000), Datum::Int16(127)),
        (Datum::Int16(9999), Datum::Int16(100)),
    ]
);
bench_binary!(
    bench_div_int16_divzero,
    DivInt16,
    func::DivInt16,
    Datum::Int16(10),
    SqlScalarType::Int16,
    Datum::Int16(0),
    SqlScalarType::Int16
);
bench_binary_multi!(
    bench_div_int32_happy,
    DivInt32,
    func::DivInt32,
    SqlScalarType::Int32,
    SqlScalarType::Int32,
    [
        (Datum::Int32(10), Datum::Int32(3)),
        (Datum::Int32(1000000), Datum::Int32(7)),
        (Datum::Int32(-500), Datum::Int32(3)),
        (Datum::Int32(i32::MAX), Datum::Int32(13)),
        (Datum::Int32(1), Datum::Int32(1)),
        (Datum::Int32(0), Datum::Int32(42)),
        (Datum::Int32(999999999), Datum::Int32(127)),
        (Datum::Int32(123456789), Datum::Int32(9876)),
    ]
);
bench_binary!(
    bench_div_int32_divzero,
    DivInt32,
    func::DivInt32,
    Datum::Int32(10),
    SqlScalarType::Int32,
    Datum::Int32(0),
    SqlScalarType::Int32
);
bench_binary_multi!(
    bench_div_int64_happy,
    DivInt64,
    func::DivInt64,
    SqlScalarType::Int64,
    SqlScalarType::Int64,
    [
        (Datum::Int64(10), Datum::Int64(3)),
        (Datum::Int64(1000000000000), Datum::Int64(7)),
        (Datum::Int64(-500), Datum::Int64(3)),
        (Datum::Int64(i64::MAX), Datum::Int64(13)),
        (Datum::Int64(1), Datum::Int64(1)),
        (Datum::Int64(0), Datum::Int64(42)),
        (Datum::Int64(999999999999), Datum::Int64(127)),
        (Datum::Int64(123456789012345), Datum::Int64(9876543)),
    ]
);
bench_binary!(
    bench_div_int64_divzero,
    DivInt64,
    func::DivInt64,
    Datum::Int64(10),
    SqlScalarType::Int64,
    Datum::Int64(0),
    SqlScalarType::Int64
);
bench_binary_multi!(
    bench_div_uint16_happy,
    DivUint16,
    func::DivUint16,
    SqlScalarType::UInt16,
    SqlScalarType::UInt16,
    [
        (Datum::UInt16(10), Datum::UInt16(3)),
        (Datum::UInt16(100), Datum::UInt16(7)),
        (Datum::UInt16(u16::MAX), Datum::UInt16(13)),
        (Datum::UInt16(1), Datum::UInt16(1)),
        (Datum::UInt16(0), Datum::UInt16(42)),
        (Datum::UInt16(60000), Datum::UInt16(127)),
    ]
);
bench_binary!(
    bench_div_uint16_divzero,
    DivUint16,
    func::DivUint16,
    Datum::UInt16(10),
    SqlScalarType::UInt16,
    Datum::UInt16(0),
    SqlScalarType::UInt16
);
bench_binary_multi!(
    bench_div_uint32_happy,
    DivUint32,
    func::DivUint32,
    SqlScalarType::UInt32,
    SqlScalarType::UInt32,
    [
        (Datum::UInt32(10), Datum::UInt32(3)),
        (Datum::UInt32(1000000), Datum::UInt32(7)),
        (Datum::UInt32(u32::MAX), Datum::UInt32(13)),
        (Datum::UInt32(1), Datum::UInt32(1)),
        (Datum::UInt32(0), Datum::UInt32(42)),
        (Datum::UInt32(999999999), Datum::UInt32(127)),
    ]
);
bench_binary!(
    bench_div_uint32_divzero,
    DivUint32,
    func::DivUint32,
    Datum::UInt32(10),
    SqlScalarType::UInt32,
    Datum::UInt32(0),
    SqlScalarType::UInt32
);
bench_binary_multi!(
    bench_div_uint64_happy,
    DivUint64,
    func::DivUint64,
    SqlScalarType::UInt64,
    SqlScalarType::UInt64,
    [
        (Datum::UInt64(10), Datum::UInt64(3)),
        (Datum::UInt64(1000000000000), Datum::UInt64(7)),
        (Datum::UInt64(u64::MAX), Datum::UInt64(13)),
        (Datum::UInt64(1), Datum::UInt64(1)),
        (Datum::UInt64(0), Datum::UInt64(42)),
        (Datum::UInt64(999999999999), Datum::UInt64(127)),
    ]
);
bench_binary!(
    bench_div_uint64_divzero,
    DivUint64,
    func::DivUint64,
    Datum::UInt64(10),
    SqlScalarType::UInt64,
    Datum::UInt64(0),
    SqlScalarType::UInt64
);
bench_binary_multi!(
    bench_div_float32_happy,
    DivFloat32,
    func::DivFloat32,
    SqlScalarType::Float32,
    SqlScalarType::Float32,
    [
        (
            Datum::Float32(OrderedFloat(10.0)),
            Datum::Float32(OrderedFloat(3.0))
        ),
        (
            Datum::Float32(OrderedFloat(1.0)),
            Datum::Float32(OrderedFloat(7.0))
        ),
        (
            Datum::Float32(OrderedFloat(1e10)),
            Datum::Float32(OrderedFloat(0.001))
        ),
        (
            Datum::Float32(OrderedFloat(-42.5)),
            Datum::Float32(OrderedFloat(3.7))
        ),
        (
            Datum::Float32(OrderedFloat(0.001)),
            Datum::Float32(OrderedFloat(1000.0))
        ),
        (
            Datum::Float32(OrderedFloat(999.999)),
            Datum::Float32(OrderedFloat(1.001))
        ),
    ]
);
bench_binary!(
    bench_div_float32_divzero,
    DivFloat32,
    func::DivFloat32,
    Datum::Float32(OrderedFloat(10.0)),
    SqlScalarType::Float32,
    Datum::Float32(OrderedFloat(0.0)),
    SqlScalarType::Float32
);
bench_binary_multi!(
    bench_div_float64_happy,
    DivFloat64,
    func::DivFloat64,
    SqlScalarType::Float64,
    SqlScalarType::Float64,
    [
        (
            Datum::Float64(OrderedFloat(10.0)),
            Datum::Float64(OrderedFloat(3.0))
        ),
        (
            Datum::Float64(OrderedFloat(1.0)),
            Datum::Float64(OrderedFloat(7.0))
        ),
        (
            Datum::Float64(OrderedFloat(1e15)),
            Datum::Float64(OrderedFloat(0.00001))
        ),
        (
            Datum::Float64(OrderedFloat(-42.5)),
            Datum::Float64(OrderedFloat(3.7))
        ),
        (
            Datum::Float64(OrderedFloat(0.001)),
            Datum::Float64(OrderedFloat(1000.0))
        ),
        (
            Datum::Float64(OrderedFloat(999.999)),
            Datum::Float64(OrderedFloat(1.001))
        ),
        (
            Datum::Float64(OrderedFloat(1.7976931e100)),
            Datum::Float64(OrderedFloat(1.23456789))
        ),
        (
            Datum::Float64(OrderedFloat(std::f64::consts::PI)),
            Datum::Float64(OrderedFloat(std::f64::consts::E))
        ),
    ]
);
bench_binary!(
    bench_div_float64_divzero,
    DivFloat64,
    func::DivFloat64,
    Datum::Float64(OrderedFloat(10.0)),
    SqlScalarType::Float64,
    Datum::Float64(OrderedFloat(0.0)),
    SqlScalarType::Float64
);
bench_binary_multi!(
    bench_div_numeric_happy,
    DivNumeric,
    func::DivNumeric,
    SqlScalarType::Numeric { max_scale: None },
    SqlScalarType::Numeric { max_scale: None },
    [
        (
            Datum::Numeric(OrderedDecimal(Numeric::from(10))),
            Datum::Numeric(OrderedDecimal(Numeric::from(3)))
        ),
        (
            Datum::Numeric(OrderedDecimal(Numeric::from(1))),
            Datum::Numeric(OrderedDecimal(Numeric::from(7)))
        ),
        (
            Datum::Numeric(OrderedDecimal(Numeric::from(1000000))),
            Datum::Numeric(OrderedDecimal(Numeric::from(13)))
        ),
        (
            Datum::Numeric(OrderedDecimal(Numeric::from(999999))),
            Datum::Numeric(OrderedDecimal(Numeric::from(42)))
        ),
        (
            Datum::Numeric(OrderedDecimal(Numeric::from(-50))),
            Datum::Numeric(OrderedDecimal(Numeric::from(3)))
        ),
        (
            Datum::Numeric(OrderedDecimal(Numeric::from(1))),
            Datum::Numeric(OrderedDecimal(Numeric::from(1)))
        ),
    ]
);
bench_binary!(
    bench_div_numeric_divzero,
    DivNumeric,
    func::DivNumeric,
    Datum::Numeric(OrderedDecimal(Numeric::from(10))),
    SqlScalarType::Numeric { max_scale: None },
    Datum::Numeric(OrderedDecimal(Numeric::from(0))),
    SqlScalarType::Numeric { max_scale: None }
);
bench_binary!(
    bench_div_interval_happy,
    DivInterval,
    func::DivInterval,
    Datum::Interval(Interval::new(2, 0, 0)),
    SqlScalarType::Interval,
    Datum::Float64(OrderedFloat(2.0)),
    SqlScalarType::Float64
);
bench_binary!(
    bench_div_interval_divzero,
    DivInterval,
    func::DivInterval,
    Datum::Interval(Interval::new(2, 0, 0)),
    SqlScalarType::Interval,
    Datum::Float64(OrderedFloat(0.0)),
    SqlScalarType::Float64
);

// --- Mod ---
bench_binary!(
    bench_mod_int16_happy,
    ModInt16,
    func::ModInt16,
    Datum::Int16(10),
    SqlScalarType::Int16,
    Datum::Int16(3),
    SqlScalarType::Int16
);
bench_binary!(
    bench_mod_int16_divzero,
    ModInt16,
    func::ModInt16,
    Datum::Int16(10),
    SqlScalarType::Int16,
    Datum::Int16(0),
    SqlScalarType::Int16
);
bench_binary!(
    bench_mod_int32_happy,
    ModInt32,
    func::ModInt32,
    Datum::Int32(10),
    SqlScalarType::Int32,
    Datum::Int32(3),
    SqlScalarType::Int32
);
bench_binary!(
    bench_mod_int32_divzero,
    ModInt32,
    func::ModInt32,
    Datum::Int32(10),
    SqlScalarType::Int32,
    Datum::Int32(0),
    SqlScalarType::Int32
);
bench_binary!(
    bench_mod_int64_happy,
    ModInt64,
    func::ModInt64,
    Datum::Int64(10),
    SqlScalarType::Int64,
    Datum::Int64(3),
    SqlScalarType::Int64
);
bench_binary!(
    bench_mod_int64_divzero,
    ModInt64,
    func::ModInt64,
    Datum::Int64(10),
    SqlScalarType::Int64,
    Datum::Int64(0),
    SqlScalarType::Int64
);
bench_binary!(
    bench_mod_uint16_happy,
    ModUint16,
    func::ModUint16,
    Datum::UInt16(10),
    SqlScalarType::UInt16,
    Datum::UInt16(3),
    SqlScalarType::UInt16
);
bench_binary!(
    bench_mod_uint32_happy,
    ModUint32,
    func::ModUint32,
    Datum::UInt32(10),
    SqlScalarType::UInt32,
    Datum::UInt32(3),
    SqlScalarType::UInt32
);
bench_binary!(
    bench_mod_uint64_happy,
    ModUint64,
    func::ModUint64,
    Datum::UInt64(10),
    SqlScalarType::UInt64,
    Datum::UInt64(3),
    SqlScalarType::UInt64
);
bench_binary!(
    bench_mod_float32_happy,
    ModFloat32,
    func::ModFloat32,
    Datum::Float32(OrderedFloat(10.0)),
    SqlScalarType::Float32,
    Datum::Float32(OrderedFloat(3.0)),
    SqlScalarType::Float32
);
bench_binary!(
    bench_mod_float64_happy,
    ModFloat64,
    func::ModFloat64,
    Datum::Float64(OrderedFloat(10.0)),
    SqlScalarType::Float64,
    Datum::Float64(OrderedFloat(3.0)),
    SqlScalarType::Float64
);
bench_binary!(
    bench_mod_numeric_happy,
    ModNumeric,
    func::ModNumeric,
    Datum::Numeric(OrderedDecimal(Numeric::from(10))),
    SqlScalarType::Numeric { max_scale: None },
    Datum::Numeric(OrderedDecimal(Numeric::from(3))),
    SqlScalarType::Numeric { max_scale: None }
);

// --- RoundNumeric (binary) ---
bench_binary!(
    bench_round_numeric_happy,
    RoundNumeric,
    func::RoundNumericBinary,
    Datum::Numeric(OrderedDecimal(Numeric::from(314))),
    SqlScalarType::Numeric { max_scale: None },
    Datum::Int32(2),
    SqlScalarType::Int32
);

// --- Age ---
fn bench_age_timestamp_happy(b: &mut Bencher) {
    let f = BinaryFunc::AgeTimestamp(func::AgeTimestamp);
    let ts1 = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let ts2 = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let a = lit(
        Datum::Timestamp(ts1),
        SqlScalarType::Timestamp { precision: None },
    );
    let e = lit(
        Datum::Timestamp(ts2),
        SqlScalarType::Timestamp { precision: None },
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_age_timestamp_tz_happy(b: &mut Bencher) {
    let f = BinaryFunc::AgeTimestampTz(func::AgeTimestampTz);
    let ts1 =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap())
            .unwrap();
    let ts2 =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let a = lit(
        Datum::TimestampTz(ts1),
        SqlScalarType::TimestampTz { precision: None },
    );
    let e = lit(
        Datum::TimestampTz(ts2),
        SqlScalarType::TimestampTz { precision: None },
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// ===========================================================================
// Step 4: Bitwise, comparison, and shift functions
// ===========================================================================

// --- BitAnd ---
bench_binary!(
    bench_bit_and_int16_happy,
    BitAndInt16,
    func::BitAndInt16,
    Datum::Int16(0x0F0F),
    SqlScalarType::Int16,
    Datum::Int16(0x00FF),
    SqlScalarType::Int16
);
bench_binary!(
    bench_bit_and_int32_happy,
    BitAndInt32,
    func::BitAndInt32,
    Datum::Int32(0x0F0F0F0F),
    SqlScalarType::Int32,
    Datum::Int32(0x00FF00FF),
    SqlScalarType::Int32
);
bench_binary!(
    bench_bit_and_int64_happy,
    BitAndInt64,
    func::BitAndInt64,
    Datum::Int64(0x0F0F),
    SqlScalarType::Int64,
    Datum::Int64(0x00FF),
    SqlScalarType::Int64
);
bench_binary!(
    bench_bit_and_uint16_happy,
    BitAndUint16,
    func::BitAndUint16,
    Datum::UInt16(0x0F0F),
    SqlScalarType::UInt16,
    Datum::UInt16(0x00FF),
    SqlScalarType::UInt16
);
bench_binary!(
    bench_bit_and_uint32_happy,
    BitAndUint32,
    func::BitAndUint32,
    Datum::UInt32(0x0F0F),
    SqlScalarType::UInt32,
    Datum::UInt32(0x00FF),
    SqlScalarType::UInt32
);
bench_binary!(
    bench_bit_and_uint64_happy,
    BitAndUint64,
    func::BitAndUint64,
    Datum::UInt64(0x0F0F),
    SqlScalarType::UInt64,
    Datum::UInt64(0x00FF),
    SqlScalarType::UInt64
);

// --- BitOr ---
bench_binary!(
    bench_bit_or_int16_happy,
    BitOrInt16,
    func::BitOrInt16,
    Datum::Int16(0x0F00),
    SqlScalarType::Int16,
    Datum::Int16(0x00F0),
    SqlScalarType::Int16
);
bench_binary!(
    bench_bit_or_int32_happy,
    BitOrInt32,
    func::BitOrInt32,
    Datum::Int32(0x0F00),
    SqlScalarType::Int32,
    Datum::Int32(0x00F0),
    SqlScalarType::Int32
);
bench_binary!(
    bench_bit_or_int64_happy,
    BitOrInt64,
    func::BitOrInt64,
    Datum::Int64(0x0F00),
    SqlScalarType::Int64,
    Datum::Int64(0x00F0),
    SqlScalarType::Int64
);
bench_binary!(
    bench_bit_or_uint16_happy,
    BitOrUint16,
    func::BitOrUint16,
    Datum::UInt16(0x0F00),
    SqlScalarType::UInt16,
    Datum::UInt16(0x00F0),
    SqlScalarType::UInt16
);
bench_binary!(
    bench_bit_or_uint32_happy,
    BitOrUint32,
    func::BitOrUint32,
    Datum::UInt32(0x0F00),
    SqlScalarType::UInt32,
    Datum::UInt32(0x00F0),
    SqlScalarType::UInt32
);
bench_binary!(
    bench_bit_or_uint64_happy,
    BitOrUint64,
    func::BitOrUint64,
    Datum::UInt64(0x0F00),
    SqlScalarType::UInt64,
    Datum::UInt64(0x00F0),
    SqlScalarType::UInt64
);

// --- BitXor ---
bench_binary!(
    bench_bit_xor_int16_happy,
    BitXorInt16,
    func::BitXorInt16,
    Datum::Int16(0x0FF0),
    SqlScalarType::Int16,
    Datum::Int16(0x00FF),
    SqlScalarType::Int16
);
bench_binary!(
    bench_bit_xor_int32_happy,
    BitXorInt32,
    func::BitXorInt32,
    Datum::Int32(0x0FF0),
    SqlScalarType::Int32,
    Datum::Int32(0x00FF),
    SqlScalarType::Int32
);
bench_binary!(
    bench_bit_xor_int64_happy,
    BitXorInt64,
    func::BitXorInt64,
    Datum::Int64(0x0FF0),
    SqlScalarType::Int64,
    Datum::Int64(0x00FF),
    SqlScalarType::Int64
);
bench_binary!(
    bench_bit_xor_uint16_happy,
    BitXorUint16,
    func::BitXorUint16,
    Datum::UInt16(0x0FF0),
    SqlScalarType::UInt16,
    Datum::UInt16(0x00FF),
    SqlScalarType::UInt16
);
bench_binary!(
    bench_bit_xor_uint32_happy,
    BitXorUint32,
    func::BitXorUint32,
    Datum::UInt32(0x0FF0),
    SqlScalarType::UInt32,
    Datum::UInt32(0x00FF),
    SqlScalarType::UInt32
);
bench_binary!(
    bench_bit_xor_uint64_happy,
    BitXorUint64,
    func::BitXorUint64,
    Datum::UInt64(0x0FF0),
    SqlScalarType::UInt64,
    Datum::UInt64(0x00FF),
    SqlScalarType::UInt64
);

// --- BitShiftLeft ---
bench_binary!(
    bench_bit_shift_left_int16_happy,
    BitShiftLeftInt16,
    func::BitShiftLeftInt16,
    Datum::Int16(1),
    SqlScalarType::Int16,
    Datum::Int32(4),
    SqlScalarType::Int32
);
bench_binary!(
    bench_bit_shift_left_int32_happy,
    BitShiftLeftInt32,
    func::BitShiftLeftInt32,
    Datum::Int32(1),
    SqlScalarType::Int32,
    Datum::Int32(4),
    SqlScalarType::Int32
);
bench_binary!(
    bench_bit_shift_left_int64_happy,
    BitShiftLeftInt64,
    func::BitShiftLeftInt64,
    Datum::Int64(1),
    SqlScalarType::Int64,
    Datum::Int32(4),
    SqlScalarType::Int32
);
bench_binary!(
    bench_bit_shift_left_uint16_happy,
    BitShiftLeftUint16,
    func::BitShiftLeftUint16,
    Datum::UInt16(1),
    SqlScalarType::UInt16,
    Datum::UInt32(4),
    SqlScalarType::UInt32
);
bench_binary!(
    bench_bit_shift_left_uint32_happy,
    BitShiftLeftUint32,
    func::BitShiftLeftUint32,
    Datum::UInt32(1),
    SqlScalarType::UInt32,
    Datum::UInt32(4),
    SqlScalarType::UInt32
);
bench_binary!(
    bench_bit_shift_left_uint64_happy,
    BitShiftLeftUint64,
    func::BitShiftLeftUint64,
    Datum::UInt64(1),
    SqlScalarType::UInt64,
    Datum::UInt32(4),
    SqlScalarType::UInt32
);

// --- BitShiftRight ---
bench_binary!(
    bench_bit_shift_right_int16_happy,
    BitShiftRightInt16,
    func::BitShiftRightInt16,
    Datum::Int16(256),
    SqlScalarType::Int16,
    Datum::Int32(4),
    SqlScalarType::Int32
);
bench_binary!(
    bench_bit_shift_right_int32_happy,
    BitShiftRightInt32,
    func::BitShiftRightInt32,
    Datum::Int32(256),
    SqlScalarType::Int32,
    Datum::Int32(4),
    SqlScalarType::Int32
);
bench_binary!(
    bench_bit_shift_right_int64_happy,
    BitShiftRightInt64,
    func::BitShiftRightInt64,
    Datum::Int64(256),
    SqlScalarType::Int64,
    Datum::Int32(4),
    SqlScalarType::Int32
);
bench_binary!(
    bench_bit_shift_right_uint16_happy,
    BitShiftRightUint16,
    func::BitShiftRightUint16,
    Datum::UInt16(256),
    SqlScalarType::UInt16,
    Datum::UInt32(4),
    SqlScalarType::UInt32
);
bench_binary!(
    bench_bit_shift_right_uint32_happy,
    BitShiftRightUint32,
    func::BitShiftRightUint32,
    Datum::UInt32(256),
    SqlScalarType::UInt32,
    Datum::UInt32(4),
    SqlScalarType::UInt32
);
bench_binary!(
    bench_bit_shift_right_uint64_happy,
    BitShiftRightUint64,
    func::BitShiftRightUint64,
    Datum::UInt64(256),
    SqlScalarType::UInt64,
    Datum::UInt32(4),
    SqlScalarType::UInt32
);

// --- Comparison ---
bench_binary!(
    bench_eq_happy,
    Eq,
    func::Eq,
    Datum::Int32(42),
    SqlScalarType::Int32,
    Datum::Int32(42),
    SqlScalarType::Int32
);
bench_binary!(
    bench_not_eq_happy,
    NotEq,
    func::NotEq,
    Datum::Int32(42),
    SqlScalarType::Int32,
    Datum::Int32(43),
    SqlScalarType::Int32
);
bench_binary!(
    bench_lt_happy,
    Lt,
    func::Lt,
    Datum::Int32(1),
    SqlScalarType::Int32,
    Datum::Int32(2),
    SqlScalarType::Int32
);
bench_binary!(
    bench_lte_happy,
    Lte,
    func::Lte,
    Datum::Int32(1),
    SqlScalarType::Int32,
    Datum::Int32(2),
    SqlScalarType::Int32
);
bench_binary!(
    bench_gt_happy,
    Gt,
    func::Gt,
    Datum::Int32(2),
    SqlScalarType::Int32,
    Datum::Int32(1),
    SqlScalarType::Int32
);
bench_binary!(
    bench_gte_happy,
    Gte,
    func::Gte,
    Datum::Int32(2),
    SqlScalarType::Int32,
    Datum::Int32(1),
    SqlScalarType::Int32
);

// ===========================================================================
// Step 5: Remaining binary functions
// ===========================================================================

// --- String functions ---
bench_binary!(
    bench_text_concat_happy,
    TextConcat,
    func::TextConcatBinary,
    Datum::String("hello"),
    SqlScalarType::String,
    Datum::String(" world"),
    SqlScalarType::String
);
bench_binary!(
    bench_position_happy,
    Position,
    func::Position,
    Datum::String("hello world"),
    SqlScalarType::String,
    Datum::String("world"),
    SqlScalarType::String
);
bench_binary!(
    bench_left_happy,
    Left,
    func::Left,
    Datum::String("hello world"),
    SqlScalarType::String,
    Datum::Int32(5),
    SqlScalarType::Int32
);
bench_binary!(
    bench_right_happy,
    Right,
    func::Right,
    Datum::String("hello world"),
    SqlScalarType::String,
    Datum::Int32(5),
    SqlScalarType::Int32
);
bench_binary!(
    bench_repeat_string_happy,
    RepeatString,
    func::RepeatString,
    Datum::String("ab"),
    SqlScalarType::String,
    Datum::Int32(3),
    SqlScalarType::Int32
);
bench_binary!(
    bench_trim_happy,
    Trim,
    func::Trim,
    Datum::String("  hello  "),
    SqlScalarType::String,
    Datum::String(" "),
    SqlScalarType::String
);
bench_binary!(
    bench_trim_leading_happy,
    TrimLeading,
    func::TrimLeading,
    Datum::String("  hello  "),
    SqlScalarType::String,
    Datum::String(" "),
    SqlScalarType::String
);
bench_binary!(
    bench_trim_trailing_happy,
    TrimTrailing,
    func::TrimTrailing,
    Datum::String("  hello  "),
    SqlScalarType::String,
    Datum::String(" "),
    SqlScalarType::String
);
bench_binary!(
    bench_normalize_happy,
    Normalize,
    func::Normalize,
    Datum::String("hello"),
    SqlScalarType::String,
    Datum::String("NFC"),
    SqlScalarType::String
);
bench_binary!(
    bench_starts_with_happy,
    StartsWith,
    func::StartsWith,
    Datum::String("hello world"),
    SqlScalarType::String,
    Datum::String("hello"),
    SqlScalarType::String
);
bench_binary!(
    bench_encoded_bytes_char_length_happy,
    EncodedBytesCharLength,
    func::EncodedBytesCharLength,
    Datum::Bytes(b"hello"),
    SqlScalarType::Bytes,
    Datum::String("utf-8"),
    SqlScalarType::String
);

// --- Pattern matching ---
bench_binary!(
    bench_like_escape_happy,
    LikeEscape,
    func::LikeEscape,
    Datum::String("100%"),
    SqlScalarType::String,
    Datum::String("\\"),
    SqlScalarType::String
);
bench_binary!(
    bench_is_like_match_case_sensitive_happy,
    IsLikeMatchCaseSensitive,
    func::IsLikeMatchCaseSensitive,
    Datum::String("hello world"),
    SqlScalarType::String,
    Datum::String("%world"),
    SqlScalarType::String
);
bench_binary!(
    bench_is_like_match_case_insensitive_happy,
    IsLikeMatchCaseInsensitive,
    func::IsLikeMatchCaseInsensitive,
    Datum::String("Hello World"),
    SqlScalarType::String,
    Datum::String("%world"),
    SqlScalarType::String
);
bench_binary!(
    bench_is_regexp_match_case_sensitive_happy,
    IsRegexpMatchCaseSensitive,
    func::IsRegexpMatchCaseSensitive,
    Datum::String("hello world"),
    SqlScalarType::String,
    Datum::String("w.rld"),
    SqlScalarType::String
);
bench_binary!(
    bench_is_regexp_match_case_insensitive_happy,
    IsRegexpMatchCaseInsensitive,
    func::IsRegexpMatchCaseInsensitive,
    Datum::String("Hello World"),
    SqlScalarType::String,
    Datum::String("w.rld"),
    SqlScalarType::String
);

// --- Encoding ---
bench_binary!(
    bench_convert_from_happy,
    ConvertFrom,
    func::ConvertFrom,
    Datum::Bytes(b"hello"),
    SqlScalarType::Bytes,
    Datum::String("utf8"),
    SqlScalarType::String
);
bench_binary!(
    bench_encode_happy,
    Encode,
    func::Encode,
    Datum::Bytes(b"hello"),
    SqlScalarType::Bytes,
    Datum::String("base64"),
    SqlScalarType::String
);
bench_binary!(
    bench_decode_happy,
    Decode,
    func::Decode,
    Datum::String("aGVsbG8="),
    SqlScalarType::String,
    Datum::String("base64"),
    SqlScalarType::String
);

// --- Digest ---
bench_binary!(
    bench_digest_string_happy,
    DigestString,
    func::DigestString,
    Datum::String("hello"),
    SqlScalarType::String,
    Datum::String("md5"),
    SqlScalarType::String
);
bench_binary!(
    bench_digest_bytes_happy,
    DigestBytes,
    func::DigestBytes,
    Datum::Bytes(b"hello"),
    SqlScalarType::Bytes,
    Datum::String("md5"),
    SqlScalarType::String
);

// --- Numeric operations ---
bench_binary_multi!(
    bench_log_numeric_happy,
    LogNumeric,
    func::LogBaseNumeric,
    SqlScalarType::Numeric { max_scale: None },
    SqlScalarType::Numeric { max_scale: None },
    [
        (
            Datum::Numeric(OrderedDecimal(Numeric::from(10))),
            Datum::Numeric(OrderedDecimal(Numeric::from(100)))
        ),
        (
            Datum::Numeric(OrderedDecimal(Numeric::from(2))),
            Datum::Numeric(OrderedDecimal(Numeric::from(1024)))
        ),
        (
            Datum::Numeric(OrderedDecimal(Numeric::from(10))),
            Datum::Numeric(OrderedDecimal(Numeric::from(42)))
        ),
        (
            Datum::Numeric(OrderedDecimal(Numeric::from(3))),
            Datum::Numeric(OrderedDecimal(Numeric::from(81)))
        ),
        (
            Datum::Numeric(OrderedDecimal(Numeric::from(7))),
            Datum::Numeric(OrderedDecimal(Numeric::from(999999)))
        ),
    ]
);
bench_binary_multi!(
    bench_power_happy,
    Power,
    func::Power,
    SqlScalarType::Float64,
    SqlScalarType::Float64,
    [
        (
            Datum::Float64(OrderedFloat(2.0)),
            Datum::Float64(OrderedFloat(10.0))
        ),
        (
            Datum::Float64(OrderedFloat(2.0)),
            Datum::Float64(OrderedFloat(0.5))
        ),
        (
            Datum::Float64(OrderedFloat(10.0)),
            Datum::Float64(OrderedFloat(3.0))
        ),
        (
            Datum::Float64(OrderedFloat(1.5)),
            Datum::Float64(OrderedFloat(7.3))
        ),
        (
            Datum::Float64(OrderedFloat(0.5)),
            Datum::Float64(OrderedFloat(20.0))
        ),
        (
            Datum::Float64(OrderedFloat(100.0)),
            Datum::Float64(OrderedFloat(0.1))
        ),
        (
            Datum::Float64(OrderedFloat(std::f64::consts::E)),
            Datum::Float64(OrderedFloat(3.0))
        ),
        (
            Datum::Float64(OrderedFloat(9.0)),
            Datum::Float64(OrderedFloat(0.5))
        ),
    ]
);
bench_binary_multi!(
    bench_power_numeric_happy,
    PowerNumeric,
    func::PowerNumeric,
    SqlScalarType::Numeric { max_scale: None },
    SqlScalarType::Numeric { max_scale: None },
    [
        (
            Datum::Numeric(OrderedDecimal(Numeric::from(2))),
            Datum::Numeric(OrderedDecimal(Numeric::from(10)))
        ),
        (
            Datum::Numeric(OrderedDecimal(Numeric::from(10))),
            Datum::Numeric(OrderedDecimal(Numeric::from(3)))
        ),
        (
            Datum::Numeric(OrderedDecimal(Numeric::from(3))),
            Datum::Numeric(OrderedDecimal(Numeric::from(7)))
        ),
        (
            Datum::Numeric(OrderedDecimal(Numeric::from(7))),
            Datum::Numeric(OrderedDecimal(Numeric::from(5)))
        ),
        (
            Datum::Numeric(OrderedDecimal(Numeric::from(100))),
            Datum::Numeric(OrderedDecimal(Numeric::from(2)))
        ),
    ]
);

// --- Byte operations ---
bench_binary!(
    bench_get_bit_happy,
    GetBit,
    func::GetBit,
    Datum::Bytes(b"\xff"),
    SqlScalarType::Bytes,
    Datum::Int32(3),
    SqlScalarType::Int32
);
bench_binary!(
    bench_get_byte_happy,
    GetByte,
    func::GetByte,
    Datum::Bytes(b"hello"),
    SqlScalarType::Bytes,
    Datum::Int32(0),
    SqlScalarType::Int32
);
bench_binary!(
    bench_constant_time_eq_bytes_happy,
    ConstantTimeEqBytes,
    func::ConstantTimeEqBytes,
    Datum::Bytes(b"hello"),
    SqlScalarType::Bytes,
    Datum::Bytes(b"hello"),
    SqlScalarType::Bytes
);
bench_binary!(
    bench_constant_time_eq_string_happy,
    ConstantTimeEqString,
    func::ConstantTimeEqString,
    Datum::String("hello"),
    SqlScalarType::String,
    Datum::String("hello"),
    SqlScalarType::String
);

// --- MzRenderTypmod ---
bench_binary!(
    bench_mz_render_typmod_happy,
    MzRenderTypmod,
    func::MzRenderTypmod,
    Datum::UInt32(23),
    SqlScalarType::UInt32,
    Datum::Int32(-1),
    SqlScalarType::Int32
);

// --- ParseIdent ---
bench_binary!(
    bench_parse_ident_happy,
    ParseIdent,
    func::ParseIdent,
    Datum::String("myschema.mytable"),
    SqlScalarType::String,
    Datum::True,
    SqlScalarType::Bool
);

// --- PrettySql ---
bench_binary!(
    bench_pretty_sql_happy,
    PrettySql,
    func::PrettySql,
    Datum::String("SELECT 1"),
    SqlScalarType::String,
    Datum::Int32(80),
    SqlScalarType::Int32
);

// --- RegexpReplace ---
fn bench_regexp_replace_happy(b: &mut Bencher) {
    let regex = mz_repr::adt::regex::Regex::new("world", false).unwrap();
    let f = BinaryFunc::RegexpReplace(func::RegexpReplace { regex, limit: 0 });
    let a = lit(Datum::String("hello world"), SqlScalarType::String);
    let e = lit(Datum::String("earth"), SqlScalarType::String);
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// --- UuidGenerateV5 ---
bench_binary!(
    bench_uuid_generate_v5_happy,
    UuidGenerateV5,
    func::UuidGenerateV5,
    Datum::Uuid(uuid::Uuid::nil()),
    SqlScalarType::Uuid,
    Datum::String("test"),
    SqlScalarType::String
);

// --- MzAclItemContainsPrivilege ---
// Skipped: requires MzAclItem datum construction

// --- ToChar ---
fn bench_to_char_timestamp_happy(b: &mut Bencher) {
    let f = BinaryFunc::ToCharTimestamp(func::ToCharTimestampFormat);
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap(),
    )
    .unwrap();
    let a = lit(
        Datum::Timestamp(ts),
        SqlScalarType::Timestamp { precision: None },
    );
    let e = lit(
        Datum::String("YYYY-MM-DD HH24:MI:SS"),
        SqlScalarType::String,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_to_char_timestamp_tz_happy(b: &mut Bencher) {
    let f = BinaryFunc::ToCharTimestampTz(func::ToCharTimestampTzFormat);
    let ts = CheckedTimestamp::from_timestamplike(
        Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap(),
    )
    .unwrap();
    let a = lit(
        Datum::TimestampTz(ts),
        SqlScalarType::TimestampTz { precision: None },
    );
    let e = lit(
        Datum::String("YYYY-MM-DD HH24:MI:SS"),
        SqlScalarType::String,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// --- DateBin ---
fn bench_date_bin_timestamp_happy(b: &mut Bencher) {
    let f = BinaryFunc::DateBinTimestamp(func::DateBinTimestamp);
    let a = lit(
        Datum::Interval(Interval::new(0, 0, 3_600_000_000)),
        SqlScalarType::Interval,
    );
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap(),
    )
    .unwrap();
    let e = lit(
        Datum::Timestamp(ts),
        SqlScalarType::Timestamp { precision: None },
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_date_bin_timestamp_tz_happy(b: &mut Bencher) {
    let f = BinaryFunc::DateBinTimestampTz(func::DateBinTimestampTz);
    let a = lit(
        Datum::Interval(Interval::new(0, 0, 3_600_000_000)),
        SqlScalarType::Interval,
    );
    let ts = CheckedTimestamp::from_timestamplike(
        Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap(),
    )
    .unwrap();
    let e = lit(
        Datum::TimestampTz(ts),
        SqlScalarType::TimestampTz { precision: None },
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// --- Extract/DatePart ---
fn bench_extract_interval_happy(b: &mut Bencher) {
    let f = BinaryFunc::ExtractInterval(func::DatePartIntervalNumeric);
    let a = lit(Datum::String("epoch"), SqlScalarType::String);
    let e = lit(
        Datum::Interval(Interval::new(1, 2, 3_000_000)),
        SqlScalarType::Interval,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_extract_time_happy(b: &mut Bencher) {
    let f = BinaryFunc::ExtractTime(func::DatePartTimeNumeric);
    let a = lit(Datum::String("hour"), SqlScalarType::String);
    let e = lit(
        Datum::Time(NaiveTime::from_hms_opt(12, 30, 45).unwrap()),
        SqlScalarType::Time,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_extract_timestamp_happy(b: &mut Bencher) {
    let f = BinaryFunc::ExtractTimestamp(func::DatePartTimestampTimestampNumeric);
    let a = lit(Datum::String("year"), SqlScalarType::String);
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let e = lit(
        Datum::Timestamp(ts),
        SqlScalarType::Timestamp { precision: None },
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_extract_timestamp_tz_happy(b: &mut Bencher) {
    let f = BinaryFunc::ExtractTimestampTz(func::DatePartTimestampTimestampTzNumeric);
    let a = lit(Datum::String("year"), SqlScalarType::String);
    let ts =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let e = lit(
        Datum::TimestampTz(ts),
        SqlScalarType::TimestampTz { precision: None },
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_extract_date_happy(b: &mut Bencher) {
    let f = BinaryFunc::ExtractDate(func::ExtractDateUnits);
    let a = lit(Datum::String("year"), SqlScalarType::String);
    let e = lit(
        Datum::Date(Date::from_pg_epoch(0).unwrap()),
        SqlScalarType::Date,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_date_part_interval_happy(b: &mut Bencher) {
    let f = BinaryFunc::DatePartInterval(func::DatePartIntervalF64);
    let a = lit(Datum::String("epoch"), SqlScalarType::String);
    let e = lit(
        Datum::Interval(Interval::new(1, 2, 3_000_000)),
        SqlScalarType::Interval,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_date_part_time_happy(b: &mut Bencher) {
    let f = BinaryFunc::DatePartTime(func::DatePartTimeF64);
    let a = lit(Datum::String("hour"), SqlScalarType::String);
    let e = lit(
        Datum::Time(NaiveTime::from_hms_opt(12, 30, 45).unwrap()),
        SqlScalarType::Time,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_date_part_timestamp_happy(b: &mut Bencher) {
    let f = BinaryFunc::DatePartTimestamp(func::DatePartTimestampTimestampF64);
    let a = lit(Datum::String("year"), SqlScalarType::String);
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let e = lit(
        Datum::Timestamp(ts),
        SqlScalarType::Timestamp { precision: None },
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_date_part_timestamp_tz_happy(b: &mut Bencher) {
    let f = BinaryFunc::DatePartTimestampTz(func::DatePartTimestampTimestampTzF64);
    let a = lit(Datum::String("year"), SqlScalarType::String);
    let ts =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let e = lit(
        Datum::TimestampTz(ts),
        SqlScalarType::TimestampTz { precision: None },
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// --- DateTrunc ---
fn bench_date_trunc_timestamp_happy(b: &mut Bencher) {
    let f = BinaryFunc::DateTruncTimestamp(func::DateTruncUnitsTimestamp);
    let a = lit(Datum::String("hour"), SqlScalarType::String);
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap(),
    )
    .unwrap();
    let e = lit(
        Datum::Timestamp(ts),
        SqlScalarType::Timestamp { precision: None },
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_date_trunc_timestamp_tz_happy(b: &mut Bencher) {
    let f = BinaryFunc::DateTruncTimestampTz(func::DateTruncUnitsTimestampTz);
    let a = lit(Datum::String("hour"), SqlScalarType::String);
    let ts = CheckedTimestamp::from_timestamplike(
        Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap(),
    )
    .unwrap();
    let e = lit(
        Datum::TimestampTz(ts),
        SqlScalarType::TimestampTz { precision: None },
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_date_trunc_interval_happy(b: &mut Bencher) {
    let f = BinaryFunc::DateTruncInterval(func::DateTruncInterval);
    let a = lit(Datum::String("hour"), SqlScalarType::String);
    let e = lit(
        Datum::Interval(Interval::new(0, 0, 5_400_000_000)),
        SqlScalarType::Interval,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// --- Timezone ---
fn bench_timezone_timestamp_binary_happy(b: &mut Bencher) {
    let f = BinaryFunc::TimezoneTimestampBinary(func::TimezoneTimestampBinary);
    let a = lit(Datum::String("UTC"), SqlScalarType::String);
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let e = lit(
        Datum::Timestamp(ts),
        SqlScalarType::Timestamp { precision: None },
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_timezone_timestamp_tz_binary_happy(b: &mut Bencher) {
    let f = BinaryFunc::TimezoneTimestampTzBinary(func::TimezoneTimestampTzBinary);
    let a = lit(Datum::String("UTC"), SqlScalarType::String);
    let ts =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let e = lit(
        Datum::TimestampTz(ts),
        SqlScalarType::TimestampTz { precision: None },
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_timezone_interval_timestamp_binary_happy(b: &mut Bencher) {
    let f = BinaryFunc::TimezoneIntervalTimestampBinary(func::TimezoneIntervalTimestampBinary);
    let a = lit(
        Datum::Interval(Interval::new(0, 0, 3_600_000_000)),
        SqlScalarType::Interval,
    );
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let e = lit(
        Datum::Timestamp(ts),
        SqlScalarType::Timestamp { precision: None },
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_timezone_interval_timestamp_tz_binary_happy(b: &mut Bencher) {
    let f = BinaryFunc::TimezoneIntervalTimestampTzBinary(func::TimezoneIntervalTimestampTzBinary);
    let a = lit(
        Datum::Interval(Interval::new(0, 0, 3_600_000_000)),
        SqlScalarType::Interval,
    );
    let ts =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let e = lit(
        Datum::TimestampTz(ts),
        SqlScalarType::TimestampTz { precision: None },
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_timezone_interval_time_binary_happy(b: &mut Bencher) {
    let f = BinaryFunc::TimezoneIntervalTimeBinary(func::TimezoneIntervalTimeBinary);
    let a = lit(
        Datum::Interval(Interval::new(0, 0, 3_600_000_000)),
        SqlScalarType::Interval,
    );
    let e = lit(
        Datum::Time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
        SqlScalarType::Time,
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_timezone_offset_happy(b: &mut Bencher) {
    let f = BinaryFunc::TimezoneOffset(func::TimezoneOffset);
    let a = lit(Datum::String("UTC"), SqlScalarType::String);
    let ts =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let e = lit(
        Datum::TimestampTz(ts),
        SqlScalarType::TimestampTz { precision: None },
    );
    let arena = RowArena::new();
    let datums: &[Datum] = &[];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// Note: Jsonb, Array, List, Map, and Range functions are skipped because they
// require complex datum construction (nested Row packing). These would need
// hand-written benchmarks with RowArena::make_datum.

// ===========================================================================
// Benchmark registration
// ===========================================================================

benchmark_group!(
    arithmetic_benches,
    // Add
    bench_add_int16_happy,
    bench_add_int16_error,
    bench_add_int32_happy,
    bench_add_int32_error,
    bench_add_int64_happy,
    bench_add_int64_error,
    bench_add_uint16_happy,
    bench_add_uint16_error,
    bench_add_uint32_happy,
    bench_add_uint32_error,
    bench_add_uint64_happy,
    bench_add_uint64_error,
    bench_add_float32_happy,
    bench_add_float32_error,
    bench_add_float64_happy,
    bench_add_float64_error,
    bench_add_numeric_happy,
    bench_add_interval_happy,
    bench_add_timestamp_interval_happy,
    bench_add_timestamp_tz_interval_happy,
    bench_add_date_interval_happy,
    bench_add_date_time_happy,
    bench_add_time_interval_happy,
    // Sub
    bench_sub_int16_happy,
    bench_sub_int16_error,
    bench_sub_int32_happy,
    bench_sub_int32_error,
    bench_sub_int64_happy,
    bench_sub_int64_error,
    bench_sub_uint16_happy,
    bench_sub_uint16_error,
    bench_sub_uint32_happy,
    bench_sub_uint32_error,
    bench_sub_uint64_happy,
    bench_sub_uint64_error,
    bench_sub_float32_happy,
    bench_sub_float64_happy,
    bench_sub_numeric_happy,
    bench_sub_interval_happy,
    bench_sub_timestamp_happy,
    bench_sub_timestamp_tz_happy,
    bench_sub_timestamp_interval_happy,
    bench_sub_timestamp_tz_interval_happy,
    bench_sub_date_happy,
    bench_sub_date_interval_happy,
    bench_sub_time_happy,
    bench_sub_time_interval_happy,
    // Mul
    bench_mul_int16_happy,
    bench_mul_int16_error,
    bench_mul_int32_happy,
    bench_mul_int32_error,
    bench_mul_int64_happy,
    bench_mul_int64_error,
    bench_mul_uint16_happy,
    bench_mul_uint16_error,
    bench_mul_uint32_happy,
    bench_mul_uint32_error,
    bench_mul_uint64_happy,
    bench_mul_uint64_error,
    bench_mul_float32_happy,
    bench_mul_float32_error,
    bench_mul_float64_happy,
    bench_mul_float64_error,
    bench_mul_numeric_happy,
    bench_mul_interval_happy,
    // Div
    bench_div_int16_happy,
    bench_div_int16_divzero,
    bench_div_int32_happy,
    bench_div_int32_divzero,
    bench_div_int64_happy,
    bench_div_int64_divzero,
    bench_div_uint16_happy,
    bench_div_uint16_divzero,
    bench_div_uint32_happy,
    bench_div_uint32_divzero,
    bench_div_uint64_happy,
    bench_div_uint64_divzero,
    bench_div_float32_happy,
    bench_div_float32_divzero,
    bench_div_float64_happy,
    bench_div_float64_divzero,
    bench_div_numeric_happy,
    bench_div_numeric_divzero,
    bench_div_interval_happy,
    bench_div_interval_divzero,
    // Mod
    bench_mod_int16_happy,
    bench_mod_int16_divzero,
    bench_mod_int32_happy,
    bench_mod_int32_divzero,
    bench_mod_int64_happy,
    bench_mod_int64_divzero,
    bench_mod_uint16_happy,
    bench_mod_uint32_happy,
    bench_mod_uint64_happy,
    bench_mod_float32_happy,
    bench_mod_float64_happy,
    bench_mod_numeric_happy,
    // Round, Age
    bench_round_numeric_happy,
    bench_age_timestamp_happy,
    bench_age_timestamp_tz_happy
);

benchmark_group!(
    bitwise_comparison_benches,
    // BitAnd
    bench_bit_and_int16_happy,
    bench_bit_and_int32_happy,
    bench_bit_and_int64_happy,
    bench_bit_and_uint16_happy,
    bench_bit_and_uint32_happy,
    bench_bit_and_uint64_happy,
    // BitOr
    bench_bit_or_int16_happy,
    bench_bit_or_int32_happy,
    bench_bit_or_int64_happy,
    bench_bit_or_uint16_happy,
    bench_bit_or_uint32_happy,
    bench_bit_or_uint64_happy,
    // BitXor
    bench_bit_xor_int16_happy,
    bench_bit_xor_int32_happy,
    bench_bit_xor_int64_happy,
    bench_bit_xor_uint16_happy,
    bench_bit_xor_uint32_happy,
    bench_bit_xor_uint64_happy,
    // BitShiftLeft
    bench_bit_shift_left_int16_happy,
    bench_bit_shift_left_int32_happy,
    bench_bit_shift_left_int64_happy,
    bench_bit_shift_left_uint16_happy,
    bench_bit_shift_left_uint32_happy,
    bench_bit_shift_left_uint64_happy,
    // BitShiftRight
    bench_bit_shift_right_int16_happy,
    bench_bit_shift_right_int32_happy,
    bench_bit_shift_right_int64_happy,
    bench_bit_shift_right_uint16_happy,
    bench_bit_shift_right_uint32_happy,
    bench_bit_shift_right_uint64_happy,
    // Comparison
    bench_eq_happy,
    bench_not_eq_happy,
    bench_lt_happy,
    bench_lte_happy,
    bench_gt_happy,
    bench_gte_happy
);

benchmark_group!(
    remaining_benches,
    // String
    bench_text_concat_happy,
    bench_position_happy,
    bench_left_happy,
    bench_right_happy,
    bench_repeat_string_happy,
    bench_trim_happy,
    bench_trim_leading_happy,
    bench_trim_trailing_happy,
    bench_normalize_happy,
    bench_starts_with_happy,
    bench_encoded_bytes_char_length_happy,
    // Pattern matching
    bench_like_escape_happy,
    bench_is_like_match_case_sensitive_happy,
    bench_is_like_match_case_insensitive_happy,
    bench_is_regexp_match_case_sensitive_happy,
    bench_is_regexp_match_case_insensitive_happy,
    // Encoding
    bench_convert_from_happy,
    bench_encode_happy,
    bench_decode_happy,
    // Digest
    bench_digest_string_happy,
    bench_digest_bytes_happy,
    // Numeric operations
    bench_log_numeric_happy,
    bench_power_happy,
    bench_power_numeric_happy,
    // Byte operations
    bench_get_bit_happy,
    bench_get_byte_happy,
    bench_constant_time_eq_bytes_happy,
    bench_constant_time_eq_string_happy,
    // Misc
    bench_mz_render_typmod_happy,
    bench_parse_ident_happy,
    bench_pretty_sql_happy,
    bench_regexp_replace_happy,
    bench_uuid_generate_v5_happy,
    // ToChar
    bench_to_char_timestamp_happy,
    bench_to_char_timestamp_tz_happy,
    // DateBin
    bench_date_bin_timestamp_happy,
    bench_date_bin_timestamp_tz_happy,
    // Extract/DatePart
    bench_extract_interval_happy,
    bench_extract_time_happy,
    bench_extract_timestamp_happy,
    bench_extract_timestamp_tz_happy,
    bench_extract_date_happy,
    bench_date_part_interval_happy,
    bench_date_part_time_happy,
    bench_date_part_timestamp_happy,
    bench_date_part_timestamp_tz_happy,
    // DateTrunc
    bench_date_trunc_timestamp_happy,
    bench_date_trunc_timestamp_tz_happy,
    bench_date_trunc_interval_happy,
    // Timezone
    bench_timezone_timestamp_binary_happy,
    bench_timezone_timestamp_tz_binary_happy,
    bench_timezone_interval_timestamp_binary_happy,
    bench_timezone_interval_timestamp_tz_binary_happy,
    bench_timezone_interval_time_binary_happy,
    bench_timezone_offset_happy
);

benchmark_main!(
    arithmetic_benches,
    bitwise_comparison_benches,
    remaining_benches
);

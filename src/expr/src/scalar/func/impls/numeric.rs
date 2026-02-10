// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::sync::LazyLock;

use dec::{OrderedDecimal, Rounding};
use mz_expr_derive::sqlfunc;
use mz_lowertest::MzReflect;
use mz_repr::adt::numeric::{self, Numeric, NumericMaxScale};
use mz_repr::{SqlColumnType, SqlScalarType, strconv};
use serde::{Deserialize, Serialize};

use crate::EvalError;
use crate::scalar::func::EagerUnaryFunc;

#[sqlfunc(
    sqlname = "-",
    preserves_uniqueness = true,
    inverse = to_unary!(NegNumeric),
    is_monotone = true
)]
fn neg_numeric(mut a: Numeric) -> Numeric {
    numeric::cx_datum().neg(&mut a);
    numeric::munge_numeric(&mut a).unwrap();
    a
}

#[sqlfunc(sqlname = "abs")]
fn abs_numeric(mut a: Numeric) -> Numeric {
    numeric::cx_datum().abs(&mut a);
    a
}

#[sqlfunc(sqlname = "ceilnumeric", is_monotone = true)]
fn ceil_numeric(mut a: Numeric) -> Numeric {
    // ceil will be nop if has no fractional digits.
    if a.exponent() >= 0 {
        return a;
    }
    let mut cx = numeric::cx_datum();
    cx.set_rounding(Rounding::Ceiling);
    cx.round(&mut a);
    numeric::munge_numeric(&mut a).unwrap();
    a
}

#[sqlfunc(sqlname = "expnumeric")]
fn exp_numeric(mut a: Numeric) -> Result<Numeric, EvalError> {
    let mut cx = numeric::cx_datum();
    cx.exp(&mut a);
    let cx_status = cx.status();
    if cx_status.overflow() {
        Err(EvalError::FloatOverflow)
    } else if cx_status.subnormal() {
        Err(EvalError::FloatUnderflow)
    } else {
        numeric::munge_numeric(&mut a).unwrap();
        Ok(a)
    }
}

#[sqlfunc(sqlname = "floornumeric", is_monotone = true)]
fn floor_numeric(mut a: Numeric) -> Numeric {
    // floor will be nop if has no fractional digits.
    if a.exponent() >= 0 {
        return a;
    }
    let mut cx = numeric::cx_datum();
    cx.set_rounding(Rounding::Floor);
    cx.round(&mut a);
    numeric::munge_numeric(&mut a).unwrap();
    a
}

fn log_guard_numeric(val: &Numeric, function_name: &str) -> Result<(), EvalError> {
    if val.is_negative() {
        return Err(EvalError::NegativeOutOfDomain(function_name.into()));
    }
    if val.is_zero() {
        return Err(EvalError::ZeroOutOfDomain(function_name.into()));
    }
    Ok(())
}

/// Precomputed ln(10) to 39 significant digits.
static LN_10: LazyLock<Numeric> = LazyLock::new(|| {
    numeric::cx_datum()
        .parse("2.30258509299404568401799145468436420760")
        .unwrap()
});

/// Precomputed 1/ln(10) = log10(e) to 39 significant digits.
static LOG10_E: LazyLock<Numeric> = LazyLock::new(|| {
    numeric::cx_datum()
        .parse("0.434294481903251827651128918916605082294")
        .unwrap()
});

/// Range reduction thresholds and their ln values.
/// Entry j contains (10^(1/2^(j+1)), ln(10)/2^(j+1)) computed via repeated sqrt.
static RANGE_STEPS: LazyLock<Vec<(Numeric, Numeric)>> = LazyLock::new(|| {
    let mut cx = numeric::cx_datum();
    let two = Numeric::from(2);
    let mut threshold = Numeric::from(10);
    let mut ln_val = LN_10.clone();
    (0..5)
        .map(|_| {
            cx.sqrt(&mut threshold);
            cx.div(&mut ln_val, &two);
            (threshold.clone(), ln_val.clone())
        })
        .collect()
});

/// Compute ln(m) for m close to 1 (specifically m in [1, 10^(1/32)) ≈ [1, 1.075))
/// using the arctanh series: ln(m) = 2 * (u + u³/3 + u⁵/5 + ...)
/// where u = (m-1)/(m+1).
fn ln_arctanh_series(cx: &mut dec::Context<Numeric>, m: &Numeric) -> Numeric {
    let one = Numeric::from(1);
    if *m == one {
        return Numeric::from(0);
    }

    let mut num = m.clone();
    cx.sub(&mut num, &one); // m - 1
    let mut den = m.clone();
    cx.add(&mut den, &one); // m + 1
    cx.div(&mut num, &den); // u = (m-1)/(m+1)

    let mut u_sq = num.clone();
    cx.mul(&mut u_sq, &num); // u²

    let mut term = num.clone(); // u^(2i+1), starts at u
    let mut sum = num; // running sum, starts at u

    for i in 1i32..50 {
        cx.mul(&mut term, &u_sq); // term = u^(2i+1)
        let divisor = Numeric::from(2 * i + 1);
        let mut contribution = term.clone();
        cx.div(&mut contribution, &divisor);
        let prev = sum.clone();
        cx.add(&mut sum, &contribution);
        if sum == prev {
            break;
        }
    }

    let two = Numeric::from(2);
    cx.mul(&mut sum, &two); // ln(m) = 2 * sum
    sum
}

/// Compute ln(x) for positive, finite x using range reduction and arctanh series.
///
/// Algorithm:
/// 1. Range reduce: x = m * 10^k where m in [1, 10)
/// 2. Further reduce m by dividing by 10^(1/2^j) for j=1..5 to bring it
///    into [1, ~1.075), tracking ln adjustments
/// 3. Compute ln(m_reduced) via arctanh Taylor series (~13 terms)
/// 4. Result: ln(x) = k*ln(10) + adjustments + ln(m_reduced)
fn ln_fast(cx: &mut dec::Context<Numeric>, x: &Numeric) -> Numeric {
    // Delegate special values (NaN, Infinity) to the C library
    if x.is_special() {
        let mut result = x.clone();
        cx.ln(&mut result);
        return result;
    }

    // Step 1: Extract integer log10 and normalize m to [1, 10)
    let d = x.digits() as i32;
    let e = x.exponent();
    let k = d - 1 + e;
    let mut m = x.clone();
    if k != 0 {
        let neg_k = Numeric::from(-k);
        cx.scaleb(&mut m, &neg_k);
    }

    // Step 2: Sub-range reduction using precomputed thresholds
    let mut ln_adj = Numeric::from(0);
    for (threshold, ln_value) in RANGE_STEPS.iter() {
        if m >= *threshold {
            cx.div(&mut m, threshold);
            cx.add(&mut ln_adj, ln_value);
        }
    }

    // Step 3: Compute ln(m_reduced) via arctanh series
    let ln_m = ln_arctanh_series(cx, &m);
    cx.add(&mut ln_adj, &ln_m);

    // Step 4: Add k * ln(10)
    if k != 0 {
        let mut k_term = LN_10.clone();
        let k_num = Numeric::from(k);
        cx.mul(&mut k_term, &k_num);
        cx.add(&mut ln_adj, &k_term);
    }

    ln_adj
}

#[sqlfunc(sqlname = "lnnumeric")]
fn ln_numeric(a: Numeric) -> Result<Numeric, EvalError> {
    log_guard_numeric(&a, "ln")?;
    let mut cx = numeric::cx_datum();
    let mut result = ln_fast(&mut cx, &a);
    numeric::munge_numeric(&mut result).unwrap();
    Ok(result)
}

#[sqlfunc(sqlname = "log10numeric")]
fn log10_numeric(a: Numeric) -> Result<Numeric, EvalError> {
    log_guard_numeric(&a, "log10")?;
    let mut cx = numeric::cx_datum();

    // Fast path: exact power of 10
    let d = a.digits() as i32;
    let e = a.exponent();
    let k = d - 1 + e;
    let mut m = a.clone();
    if k != 0 {
        let neg_k = Numeric::from(-k);
        cx.scaleb(&mut m, &neg_k);
    }
    if m == Numeric::from(1) {
        let mut result = Numeric::from(k);
        numeric::munge_numeric(&mut result).unwrap();
        return Ok(result);
    }

    // General case: log10(a) = ln(a) * log10(e)
    let mut result = ln_fast(&mut cx, &a);
    cx.mul(&mut result, &LOG10_E);
    numeric::munge_numeric(&mut result).unwrap();
    Ok(result)
}

#[sqlfunc(sqlname = "roundnumeric", is_monotone = true)]
fn round_numeric(mut a: Numeric) -> Numeric {
    // round will be nop if has no fractional digits.
    if a.exponent() >= 0 {
        return a;
    }
    numeric::cx_datum().round(&mut a);
    a
}

#[sqlfunc(sqlname = "truncnumeric", is_monotone = true)]
fn trunc_numeric(mut a: Numeric) -> Numeric {
    // trunc will be nop if has no fractional digits.
    if a.exponent() >= 0 {
        return a;
    }
    let mut cx = numeric::cx_datum();
    cx.set_rounding(Rounding::Down);
    cx.round(&mut a);
    numeric::munge_numeric(&mut a).unwrap();
    a
}

#[sqlfunc(sqlname = "sqrtnumeric")]
fn sqrt_numeric(mut a: Numeric) -> Result<Numeric, EvalError> {
    if a.is_negative() {
        return Err(EvalError::NegSqrt);
    }
    let mut cx = numeric::cx_datum();
    cx.sqrt(&mut a);
    numeric::munge_numeric(&mut a).unwrap();
    Ok(a)
}

#[sqlfunc(
    sqlname = "numeric_to_smallint",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastInt16ToNumeric(None)),
    is_monotone = true
)]
pub fn cast_numeric_to_int16(mut a: Numeric) -> Result<i16, EvalError> {
    let mut cx = numeric::cx_datum();
    cx.round(&mut a);
    cx.clear_status();
    let i = cx
        .try_into_i32(a)
        .or_else(|_| Err(EvalError::Int16OutOfRange(a.to_string().into())))?;
    i16::try_from(i).or_else(|_| Err(EvalError::Int16OutOfRange(i.to_string().into())))
}

#[sqlfunc(
    sqlname = "numeric_to_integer",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastInt32ToNumeric(None)),
    is_monotone = true
)]
pub fn cast_numeric_to_int32(mut a: Numeric) -> Result<i32, EvalError> {
    let mut cx = numeric::cx_datum();
    cx.round(&mut a);
    cx.clear_status();
    cx.try_into_i32(a)
        .or_else(|_| Err(EvalError::Int32OutOfRange(a.to_string().into())))
}

#[sqlfunc(
    sqlname = "numeric_to_bigint",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastInt64ToNumeric(None)),
    is_monotone = true
)]
pub fn cast_numeric_to_int64(mut a: Numeric) -> Result<i64, EvalError> {
    let mut cx = numeric::cx_datum();
    cx.round(&mut a);
    cx.clear_status();
    cx.try_into_i64(a)
        .or_else(|_| Err(EvalError::Int64OutOfRange(a.to_string().into())))
}

#[sqlfunc(
    sqlname = "numeric_to_real",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastFloat32ToNumeric(None)),
    is_monotone = true
)]
pub fn cast_numeric_to_float32(a: Numeric) -> Result<f32, EvalError> {
    let i = a.to_string().parse::<f32>().unwrap();
    if i.is_infinite() {
        Err(EvalError::Float32OutOfRange(i.to_string().into()))
    } else {
        Ok(i)
    }
}

#[sqlfunc(
    sqlname = "numeric_to_double",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastFloat64ToNumeric(None)),
    is_monotone = true
)]
pub fn cast_numeric_to_float64(a: Numeric) -> Result<f64, EvalError> {
    let i = a.to_string().parse::<f64>().unwrap();
    if i.is_infinite() {
        Err(EvalError::Float64OutOfRange(i.to_string().into()))
    } else {
        Ok(i)
    }
}

#[sqlfunc(
    sqlname = "numeric_to_text",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastStringToNumeric(None))
)]
fn cast_numeric_to_string(a: Numeric) -> String {
    let mut buf = String::new();
    strconv::format_numeric(&mut buf, &OrderedDecimal(a));
    buf
}

#[sqlfunc(
    sqlname = "numeric_to_uint2",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastUint16ToNumeric(None)),
    is_monotone = true
)]
fn cast_numeric_to_uint16(mut a: Numeric) -> Result<u16, EvalError> {
    let mut cx = numeric::cx_datum();
    cx.round(&mut a);
    cx.clear_status();
    let u = cx
        .try_into_u32(a)
        .or_else(|_| Err(EvalError::UInt16OutOfRange(a.to_string().into())))?;
    u16::try_from(u).or_else(|_| Err(EvalError::UInt16OutOfRange(u.to_string().into())))
}

#[sqlfunc(
    sqlname = "numeric_to_uint4",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastUint32ToNumeric(None)),
    is_monotone = true
)]
fn cast_numeric_to_uint32(mut a: Numeric) -> Result<u32, EvalError> {
    let mut cx = numeric::cx_datum();
    cx.round(&mut a);
    cx.clear_status();
    cx.try_into_u32(a)
        .or_else(|_| Err(EvalError::UInt32OutOfRange(a.to_string().into())))
}

#[sqlfunc(
    sqlname = "numeric_to_uint8",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastUint64ToNumeric(None)),
    is_monotone = true
)]
fn cast_numeric_to_uint64(mut a: Numeric) -> Result<u64, EvalError> {
    let mut cx = numeric::cx_datum();
    cx.round(&mut a);
    cx.clear_status();
    cx.try_into_u64(a)
        .or_else(|_| Err(EvalError::UInt64OutOfRange(a.to_string().into())))
}

#[sqlfunc(sqlname = "pg_size_pretty", preserves_uniqueness = false)]
fn pg_size_pretty(mut a: Numeric) -> Result<String, EvalError> {
    let mut cx = numeric::cx_datum();
    let units = ["bytes", "kB", "MB", "GB", "TB", "PB"];

    for (pos, unit) in units.iter().rev().skip(1).rev().enumerate() {
        // return if abs(round(a)) < 10 in the next unit it would be converted to.
        if Numeric::from(-10239.5) < a && a < Numeric::from(10239.5) {
            // do not round a when the unit is bytes, as no conversion has happened.
            if pos > 0 {
                cx.round(&mut a);
            }

            return Ok(format!("{} {unit}", a.to_standard_notation_string()));
        }

        cx.div(&mut a, &Numeric::from(1024));
        numeric::munge_numeric(&mut a).unwrap();
    }

    cx.round(&mut a);
    Ok(format!(
        "{} {}",
        a.to_standard_notation_string(),
        units.last().unwrap()
    ))
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct AdjustNumericScale(pub NumericMaxScale);

impl<'a> EagerUnaryFunc<'a> for AdjustNumericScale {
    type Input = Numeric;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, mut d: Numeric) -> Result<Numeric, EvalError> {
        if numeric::rescale(&mut d, self.0.into_u8()).is_err() {
            return Err(EvalError::NumericFieldOverflow);
        };
        Ok(d)
    }

    fn output_type(&self, input: SqlColumnType) -> SqlColumnType {
        SqlScalarType::Numeric {
            max_scale: Some(self.0),
        }
        .nullable(input.nullable)
    }
}

impl fmt::Display for AdjustNumericScale {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("adjust_numeric_scale")
    }
}

#[cfg(test)]
mod fast_ln_tests {
    use super::*;

    /// Compare our fast ln/log10 against the C decNumber library.
    #[mz_ore::test]
    fn test_fast_ln_accuracy() {
        let test_values = [
            "1",
            "2",
            "3",
            "5",
            "7",
            "10",
            "42",
            "100",
            "1000",
            "99999",
            "0.001",
            "0.1",
            "0.5",
            "1.5",
            "3.14159",
            "1234567890",
            "0.000001",
            "9999999999999999999999999999999999999",
            "1.23456789012345678901234567890123456789",
        ];

        for s in &test_values {
            let mut cx = numeric::cx_datum();
            let n: Numeric = cx.parse(*s).unwrap();

            // C library reference
            let mut c_result = n.clone();
            cx.ln(&mut c_result);

            // Our fast implementation
            let mut our_result = ln_fast(&mut cx, &n);
            numeric::munge_numeric(&mut our_result).unwrap();

            // Allow up to ~10 ULP difference (last 1-2 digits)
            let mut diff = c_result.clone();
            cx.sub(&mut diff, &our_result);
            cx.abs(&mut diff);

            let mut threshold = c_result.clone();
            cx.abs(&mut threshold);
            // 1e-35 relative tolerance (4 digits of slack out of 39)
            let tol: Numeric = cx.parse("1E-35").unwrap();
            cx.mul(&mut threshold, &tol);

            assert!(
                diff <= threshold || diff.is_zero(),
                "ln({s}): C={c_result}, Ours={our_result}, diff={diff}"
            );
        }
    }

    #[mz_ore::test]
    fn test_fast_log10_accuracy() {
        let test_values = [
            "1",
            "2",
            "3",
            "5",
            "7",
            "10",
            "42",
            "100",
            "1000",
            "99999",
            "0.001",
            "0.1",
            "0.5",
            "1.5",
            "3.14159",
            "1234567890",
            "0.000001",
            "9999999999999999999999999999999999999",
            "1.23456789012345678901234567890123456789",
        ];

        for s in &test_values {
            let mut cx = numeric::cx_datum();
            let n: Numeric = cx.parse(*s).unwrap();

            // C library reference
            let mut c_result = n.clone();
            cx.log10(&mut c_result);

            // Our fast implementation
            let our_result = log10_numeric(n).unwrap();

            // Allow ~10 ULP difference
            let mut diff = c_result.clone();
            cx.sub(&mut diff, &our_result);
            cx.abs(&mut diff);

            let mut threshold = c_result.clone();
            cx.abs(&mut threshold);
            let tol: Numeric = cx.parse("1E-35").unwrap();
            cx.mul(&mut threshold, &tol);

            assert!(
                diff <= threshold || diff.is_zero(),
                "log10({s}): C={c_result}, Ours={our_result}, diff={diff}"
            );
        }
    }

    #[mz_ore::test]
    fn test_log10_exact_powers_of_10() {
        // Exact powers of 10 should return exact integer results
        for k in -10i32..=10 {
            let mut cx = numeric::cx_datum();
            let s = format!("1E{k}");
            let n: Numeric = cx.parse(s.as_str()).unwrap();
            let result = log10_numeric(n).unwrap();
            let expected = Numeric::from(k);
            assert_eq!(result, expected, "log10(1E{k}) should be exactly {k}");
        }
    }
}

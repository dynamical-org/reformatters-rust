#[must_use]
pub fn round(value: f32, keep_mantissa_bits: u32) -> f32 {
    // The IEEE 754 standard specifies a binary32 as having:
    //   Sign bit: 1 bit
    //   Exponent width: 8 bits
    //   Significand precision: 24 bits (23 explicitly stored)
    // <https://en.wikipedia.org/wiki/Single-precision_floating-point_format#IEEE_754_standard:_binary32>
    const MANTISSA_BITS: u32 = 23;
    const MANTISSA_MASK: u32 = trailing_1s(MANTISSA_BITS);
    const EXPONENT_MASK: u32 = trailing_1s(8) << (32 - 8 - 1);
    const SIGN_MASK: u32 = 1 << 31;

    if keep_mantissa_bits >= MANTISSA_BITS || value.is_nan() || value.is_infinite() {
        return value;
    }

    let bits = value.to_bits();

    // The number of trailing bits we'll be setting to zero
    let drop_bits = MANTISSA_BITS - keep_mantissa_bits;

    // Extract portions of the number's bits
    // Rounding mantissa to the n-th place (ie n == keep_bits_mantissa):
    let sign = bits & SIGN_MASK;
    let mut exponent = bits & EXPONENT_MASK;
    let mut mantissa = bits & MANTISSA_MASK;
    let round_bit = bits & (1 << drop_bits); // n-th place - the smallest bit remaining after rounding
    let half_bit = bits & (1 << (drop_bits - 1)); // n+1 th - the bit directly following round bit
    let sticky_bits = bits & trailing_1s(drop_bits - 1); // nth + 2+ - the rest of the bits after half bit

    // Via the general rule at the bottom of this post
    // https://angularindepth.com/posts/1017/how-to-round-binary-numbers
    mantissa = match (half_bit == 0, sticky_bits == 0) {
        (true, _) => round_down(mantissa, drop_bits),
        (false, false) => round_up(mantissa, drop_bits),
        (false, true) => round_to_even(mantissa, drop_bits, round_bit),
    };

    // If mantissa overflows, increment exponent and truncate overflow so mantissa wraps back to zero
    if mantissa > MANTISSA_MASK {
        exponent += 1 << MANTISSA_BITS; // add 1 to exponent
        mantissa &= MANTISSA_MASK; // zero out overflowed bits

        // if exponent overflows, return infinity
        // all 1s exponent has a special meaning, so it's also invalid as a numeric value
        if exponent >= EXPONENT_MASK {
            let sign_is_positive = sign == 0; // zero means positive, 1 means negative
            return if sign_is_positive {
                f32::INFINITY
            } else {
                f32::NEG_INFINITY
            };
        }
    }

    f32::from_bits(sign | exponent | mantissa)
}

fn round_down(value: u32, drop_bits: u32) -> u32 {
    (value >> drop_bits) << drop_bits
}

fn round_up(value: u32, drop_bits: u32) -> u32 {
    let value = round_down(value, drop_bits);
    let increment = 1 << drop_bits;
    value + increment
}

/// Tie breaking round using the "ties to even" logic.
/// See <https://en.wikipedia.org/wiki/IEEE_754#Roundings_to_nearest>
fn round_to_even(value: u32, drop_bits: u32, round_bit: u32) -> u32 {
    // if bit we're rounding to is already 0, then rounding up would make it 1 (odd)
    // so we round down and leave it zero (even).
    if round_bit == 0 {
        round_down(value, drop_bits)
    } else {
        round_up(value, drop_bits)
    }
}

const fn trailing_1s(num_ones: u32) -> u32 {
    (1 << num_ones) - 1
}

#[cfg(test)]
#[allow(
    clippy::float_cmp,
    clippy::unusual_byte_groupings,
    clippy::unreadable_literal
)]
mod tests {
    use super::*;

    #[test]
    fn round_up_no_tie_positive() {
        //                           sign exponent mantissa
        //                              - -------- -----------------------
        let original = f32::from_bits(0b0_10000000_10101010101010101010101);
        assert_eq!(original, 3.3333333_f32);

        let rounded = round(original, 4);

        assert!(rounded > original); // should have rounded up
        let expected = f32::from_bits(0b0_10000000_10110000000000000000000);
        assert_eq!(rounded, expected);
        assert_eq!(rounded, 3.375);
    }

    #[test]
    fn round_up_no_tie_negative() {
        let original = f32::from_bits(0b1_10000000_10101010101010101010101);
        assert_eq!(original, -3.3333333_f32);

        let rounded = round(original, 4);

        let expected = f32::from_bits(0b1_10000000_10110000000000000000000);
        assert_eq!(rounded, expected);
        assert_eq!(rounded, -3.375);

        assert!(rounded < original); // "up" actually means "away from zero"

        // check that rounding down would actually have gotten you farther away
        let rounded_down = f32::from_bits(0b1_10000000_10100000000000000000000);
        assert!((original - expected).abs() < (original - rounded_down).abs());
    }

    #[test]
    fn round_down_no_tie_positive() {
        let original = f32::from_bits(0b0_10000011_10101010101010101010101);
        assert_eq!(original, 26.666666);

        let rounded = round(original, 5);

        let expected = f32::from_bits(0b0_10000011_10101000000000000000000);
        assert_eq!(rounded, expected);
        assert_eq!(rounded, 26.5);

        assert!(rounded < original);

        let rounded_up = f32::from_bits(0b0_10000011_10111000000000000000000);
        // check that rounding up would actually have gotten you farther away
        assert!((original - expected).abs() < (original - rounded_up).abs());
    }

    #[test]
    fn round_down_no_tie_negative() {
        let original = f32::from_bits(0b1_10000011_10101010101010101010101);
        assert_eq!(original, -26.666666);

        let rounded = round(original, 5);

        let expected = f32::from_bits(0b1_10000011_10101000000000000000000);
        assert_eq!(rounded, expected);
        assert_eq!(rounded, -26.5);

        assert!(rounded > original);

        let rounded_up = f32::from_bits(0b1_10000011_10111000000000000000000);
        // check that rounding up would actually have gotten you farther away
        assert!((original - expected).abs() < (original - rounded_up).abs());
    }

    #[test]
    fn round_tie_down_to_even_positive() {
        let original = f32::from_bits(0b0_10000010_00100000000000000000000);
        //                             half bit is 1 ^, all else is zeros
        assert_eq!(original, 9.);

        let rounded = round(original, 2);

        let expected = f32::from_bits(0b0_10000010_00000000000000000000000);
        assert_eq!(rounded, expected);
        assert_eq!(rounded, 8.);

        assert!(rounded < original);

        let rounded_up = f32::from_bits(0b0_10000010_01000000000000000000000);
        // check that rounding up would actually have gotten you farther away
        assert!((original - expected).abs() == (original - rounded_up).abs());
    }

    #[test]
    fn round_tie_up_to_even_positive() {
        let original = f32::from_bits(0b0_10000010_00110000000000000000000);
        assert_eq!(original, 9.5);

        let rounded = round(original, 3);

        let expected = f32::from_bits(0b0_10000010_01000000000000000000000);
        assert_eq!(rounded, expected);
        assert_eq!(rounded, 10.);

        assert!(rounded > original);

        let rounded_down = f32::from_bits(0b0_10000010_00100000000000000000000);
        // check that rounding up would actually have gotten you farther away
        assert!((original - expected).abs() == (original - rounded_down).abs());
    }

    #[test]
    fn round_keep_all_bits() {
        let original = f32::from_bits(0b0_10000001_10000000000000000000111);
        assert_eq!(original, 6.0000033);

        let rounded = round(original, 23); // keep all bits, not rounding

        assert_eq!(rounded, original);
    }

    #[test]
    fn round_keep_all_but_one_bits_trailing_1() {
        let original = f32::from_bits(0b0_10000001_10000000000000000000111);
        assert_eq!(original, 6.0000033);

        let rounded = round(original, 22);

        let expected = f32::from_bits(0b0_10000001_10000000000000000001000);
        assert_eq!(rounded, expected);
    }
    #[test]
    fn round_keep_all_but_one_bits_trailing_0() {
        let original = f32::from_bits(0b0_10000001_10000000000000000000110);
        assert_eq!(original, 6.000003);

        let rounded = round(original, 22);

        let expected = f32::from_bits(0b0_10000001_10000000000000000000110);
        assert_eq!(rounded, expected);
    }
    #[test]
    fn round_mantissa_overflow() {
        let original = f32::from_bits(0b0_10000001_11111111111111111111111);
        assert_eq!(original, 7.9999995);

        let rounded = round(original, 15);

        let expected = f32::from_bits(0b0_10000010_00000000000000000000000);
        assert_eq!(rounded, 8.);
        assert_eq!(rounded, expected);
    }
    #[test]
    fn round_exponent_and_mantissa_overflow_positive() {
        let original = f32::from_bits(0b0_11111110_11111111111111111111111);

        let rounded = round(original, 20);

        assert_eq!(rounded, f32::INFINITY);
    }
    #[test]
    fn round_exponent_and_mantissa_overflow_negative() {
        let original = f32::from_bits(0b1_11111110_11111111111111111111111);

        let rounded = round(original, 20);

        assert_eq!(rounded, f32::NEG_INFINITY);
    }
    #[test]
    fn round_zero() {
        let original: f32 = 0.;

        let rounded = round(original, 20);

        assert_eq!(rounded, 0.);
    }
    #[test]
    fn round_subnormal_to_normal() {
        let original = f32::from_bits(0b0_00000000_11111111111111111111111);
        assert_eq!(original, 1.1754942e-38);

        let rounded = round(original, 20);

        let expected = f32::from_bits(0b0_00000001_00000000000000000000000);
        assert_eq!(expected, 1.1754944e-38);

        assert_eq!(rounded, expected);
    }
    #[test]
    fn round_subnormal_to_subnormal() {
        let original = f32::from_bits(0b0_00000000_00011111111111111111111);
        assert_eq!(original, 1.469367e-39);

        let rounded = round(original, 3);

        let expected = f32::from_bits(0b0_00000000_00100000000000000000000);
        assert_eq!(expected, 1.469368e-39);

        assert_eq!(rounded, expected);
    }
    #[test]
    fn print_diffs() {
        let oo = ndarray::Array::logspace(2_f32, -127., 127., 2_000);
        let mut max: f32 = 0.;
        for o in oo {
            let r = round(o, 9);
            let diff_percent = ((o - r) / o).abs() * 100.;
            if diff_percent > max {
                max = diff_percent;
            }
            println!("{diff_percent:.2} {o} {r}");
        }
        assert!(max < 0.5); // less than 1/2 of 1% Grror
        println!("max diff % {max:.3}");
    }
}

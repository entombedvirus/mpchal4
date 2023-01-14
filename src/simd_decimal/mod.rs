use std::{
    arch::x86_64::{
        __m128i, _mm_and_si128, _mm_cvtsi128_si64, _mm_loadu_si128, _mm_madd_epi16,
        _mm_maddubs_epi16, _mm_or_si128, _mm_packs_epi32, _mm_set1_epi8, _mm_setr_epi16,
        _mm_setr_epi8, _mm_setzero_si128, _mm_shuffle_epi8, _mm_slli_epi16, _mm_slli_si128,
        _mm_sub_epi8,
    },
    io::BufRead,
    mem::MaybeUninit,
};

const REG_BYTES: usize = 16;

#[inline]
pub fn parse_incomplete<const N: usize, const LINE_WIDTH: usize>(
    inputs: &[u8],
    outputs: &mut Vec<u128>,
) {
    outputs.reserve(inputs.len() + 1);
    let mut chunker = ChunkerIter::<LINE_WIDTH, REG_BYTES, N>::new(inputs);
    let mut output_ptr = outputs.as_mut_ptr_range().end;
    for chunk in &mut chunker {
        for i in 0..N {
            unsafe {
                do_parse_incomplete::<LINE_WIDTH>(chunk[i], output_ptr);
                output_ptr = output_ptr.add(1);
            }
        }
    }
    unsafe { outputs.set_len(output_ptr.sub_ptr(outputs.as_ptr())) };
    unsafe {
        for rem in chunker.remainder() {
            let ascii_num_wo_nl = rem.get_unchecked(..LINE_WIDTH - 1);
            let mut bytes = [0; 16];
            bytes[3..].copy_from_slice(ascii_num_wo_nl);
            bytes.reverse();
            let val = u128::from_le_bytes(bytes);
            outputs.push(val);
        }
    }
    // outputs.extend(chunker.remainder().map(parse_num_with_newline));
}

pub fn parse_packed_4bit<const N: usize, const LINE_WIDTH: usize>(
    inputs: &[u8],
    outputs: &mut Vec<u64>,
) {
    let expected_results = inputs.len() / LINE_WIDTH;
    assert_eq!(inputs.len() % LINE_WIDTH, 0, "only pass complete lines");

    outputs.clear();
    outputs.reserve(expected_results);

    let mut chunker = ChunkerIter::<LINE_WIDTH, REG_BYTES, N>::new(inputs);
    let mut i = 0;
    for chunk in &mut chunker {
        unsafe {
            do_parse_packed_4bit::<N, LINE_WIDTH>(
                &chunk,
                outputs.get_unchecked_mut(i..i + N).try_into().unwrap(),
            );
            i += N;
        }
    }
    unsafe {
        for rem in chunker.remainder() {
            let mut buf = [0; REG_BYTES];
            buf.get_unchecked_mut(..rem.len()).copy_from_slice(rem);
            do_parse_packed_4bit::<1, LINE_WIDTH>(
                &[&buf],
                outputs.get_unchecked_mut(i..i + 1).try_into().unwrap(),
            );
            i += 1;
        }
        outputs.set_len(expected_results);
    }
}

unsafe fn do_parse_packed_4bit<const N: usize, const LINE_WIDTH: usize>(
    inputs: &[&[u8; REG_BYTES]; N],
    outputs: &mut [u64; N],
) {
    let zero = _mm_set1_epi8(b'0' as i8);
    let and_mask = _mm_setr_epi8(
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0x00, 0x00, 0x00,
    );
    let control = _mm_setr_epi8(12, 10, 8, 6, 4, 2, 0, -1, -1, -1, -1, -1, -1, -1, -1, -1);
    let mut cleaned = [_mm_setzero_si128(); N];

    for i in 0..N {
        let a = _mm_loadu_si128(inputs[i] as *const u8 as *const __m128i);
        cleaned[i] = _mm_sub_epi8(a, zero);
    }

    for i in 0..N {
        // zero out three bytes off the end since that's not part
        // of the number
        cleaned[i] = _mm_and_si128(cleaned[i], and_mask);
    }

    for i in 0..N {
        // shift left by 4bits so that we can do a logical OR
        // to pack together 2 bytes into 1 byte
        let b = _mm_slli_epi16(cleaned[i], 4);
        // shift right by 1 byte so that things line up to do
        // the OR operation
        let b = _mm_slli_si128(b, 1);
        // do the OR operation such that two adjacent bytes
        // end up getting packed together into one byte
        cleaned[i] = _mm_or_si128(cleaned[i], b);
    }

    for i in 0..N {
        // move the bytes around such that the lower 64bits
        // contain the number that we want
        cleaned[i] = _mm_shuffle_epi8(cleaned[i], control);
    }

    for i in 0..N {
        // extract the lower 64bits as a u64
        outputs[i] = _mm_cvtsi128_si64(cleaned[i]) as u64;
    }
}

// Copied from https://github.com/vgatherps/simd_decimal/blob/main/src/parser_sse.rs#L16 and
// modified. See LICENSE for compliance details.
#[inline]
unsafe fn do_parse_incomplete<const LINE_WIDTH: usize>(input: ParseInput, output: *mut u128) {
    let ascii_num_wo_nl = input.get_unchecked(..LINE_WIDTH - 1);
    let mut bytes = [0; 16];
    bytes[3..].copy_from_slice(ascii_num_wo_nl);
    bytes.reverse();
    std::ptr::copy_nonoverlapping::<u128>(bytes.as_ptr() as *const u128, output, 1);
    // let val = u128::from_le_bytes(bytes);
    // output.write(val);
}

#[inline]
pub fn parse_decimals<const N: usize, const LINE_WIDTH: usize>(
    inputs: &[u8],
    outputs: &mut Vec<u64>,
) {
    outputs.reserve(inputs.len() / LINE_WIDTH);
    let mut chunker = ChunkerIter::<LINE_WIDTH, REG_BYTES, N>::new(inputs);
    let mut output_ptr = outputs.as_mut_ptr_range().end;
    for chunk in &mut chunker {
        unsafe {
            do_parse_decimals(&chunk, output_ptr);
            output_ptr = output_ptr.add(N);
        }
    }
    unsafe { outputs.set_len(output_ptr.sub_ptr(outputs.as_ptr())) };
    outputs.extend(chunker.remainder().map(parse_num_with_newline));
}

struct ChunkerIter<'a, const L: usize, const R: usize, const N: usize> {
    slice: &'a [u8],
}

impl<'a, const L: usize, const R: usize, const N: usize> ChunkerIter<'a, L, R, N> {
    fn new(slice: &'a [u8]) -> Self {
        assert!(L <= R, "line cannot be longer than register width");
        assert!(
            slice.len() % L == 0,
            "parse_decimals can only handle complete lines"
        );
        Self { slice }
    }

    fn remainder(self) -> impl Iterator<Item = &'a [u8; L]> {
        assert!(
            self.slice.len() % L == 0,
            "remainder is expected to handle only whole lines"
        );
        self.slice.array_chunks::<L>()
    }
}

impl<'a, const L: usize, const R: usize, const N: usize> Iterator for ChunkerIter<'a, L, R, N> {
    type Item = [&'a [u8; R]; N];

    fn next(&mut self) -> Option<Self::Item> {
        if N * R <= self.slice.len() {
            let mut arr: [MaybeUninit<&'a [u8; R]>; N] = MaybeUninit::uninit_array();
            let mut buf = self.slice.as_ptr();
            for i in 0..N {
                let as_ptr = buf as *const [u8; R];
                // SAFETY: buf is guaranteed to be a valid address because we check that L <= R
                // in the constructor and the slice is at least N * R long
                let as_ref = unsafe {
                    buf = buf.add(L);
                    &*as_ptr as &'a [u8; R]
                };
                arr[i] = MaybeUninit::new(as_ref);
            }
            self.slice.consume(N * L);
            // SAFETY: aa elements of arr is written to with valid references
            // in the loop earlier
            Some(unsafe { MaybeUninit::array_assume_init(arr) })
        } else {
            None
        }
    }
}

fn parse_num_with_newline<const L: usize>(digits: &[u8; L]) -> u64 {
    let mut res: u64 = 0;
    for &c in &digits[..digits.len() - 1] {
        res *= 10;
        let digit = (c as u64) - '0' as u64;
        res += digit;
    }
    res
}

type ParseInput<'a> = &'a [u8; REG_BYTES];

// Copied from https://github.com/vgatherps/simd_decimal/blob/main/src/parser_sse.rs#L16 and
// modified. See LICENSE for compliance details.
#[inline]
pub unsafe fn do_parse_decimals<const N: usize>(inputs: &[ParseInput; N], outputs: *mut u64) {
    let ascii = _mm_set1_epi8(b'0' as i8);
    let mut cleaned = [_mm_set1_epi8(0); N];

    // PERF
    // I did some expermients to hoist the dot-discovery code above the length shifting code,
    // to try and remove a data dependency. This surprisingly really hurt performance,
    // although in theory it should be a significant improvement as you remove a data dependency
    // from the shift to the dot discovery...

    // This is done as a series of many loops to maximise the instant parallelism available to the
    // cpu. It's semantically identical but means the decoder doesn't have to churn through
    // many copies of the code to find independent instructions

    // first, load data and subtract off the ascii mask
    // Everything in the range '0'..'9' will become 0..9
    // everthing else will overflow into 10..256
    for i in 0..N {
        // transumte will just compile to the intrinsics anyways
        let loaded = std::mem::transmute(*inputs[i]);
        cleaned[i] = _mm_sub_epi8(loaded, ascii);
    }

    // now, we convert the string from [1234.123 <garbage>] into [00000 ... 1234.123]
    // as well as insert zeros for everything past the end

    // For known-short strings, replacing this with a shift might reduce
    // contention on port 5 (the shuffle port). You can't do this for a full vector
    // since there's no way to do so without an immediate value
    for i in 0..N {
        let shift_mask = _mm_setr_epi8(-1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        cleaned[i] = _mm_shuffle_epi8(cleaned[i], shift_mask);
    }

    // Now, all that we do is convert to an actual integer

    // Take pairs of u8s (digits) and multiply the more significant one by 10,
    // and accumulate into pairwise u16
    for cl in &mut cleaned {
        let mul_1_10 = _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1);
        *cl = _mm_maddubs_epi16(*cl, mul_1_10);
    }

    // Take pairs of u16s (not digits, but two digits each)
    // multiply the more significant by 100 and add to get pairwise u32
    for cl in &mut cleaned {
        let mul_1_100 = _mm_setr_epi16(100, 1, 100, 1, 100, 1, 100, 1);
        *cl = _mm_madd_epi16(*cl, mul_1_100);
    }

    // We now have pairwise u32s, but there are no methods to multiply and horizontally add
    // them. Doing it outright is *very* slow.
    // We know that nothing yet can be larger than 2^16, so we pack the u16s
    // into the first and second half of the vector
    // Each vector half will now be identical.

    for cl in &mut cleaned {
        *cl = _mm_packs_epi32(*cl, *cl);
    }

    // Two choices with similar theoretical performance, afaik.
    // One is that we do one more round of multiply-accumulate in simd, then exit to integer
    // The other is that we do some swar games on what we've just packed into the first 64 bytes.
    // The simd one *I think* faster. Higher throughput, less instructions to issue
    // but might compete with the other madd slots a but more
    // The swar one:
    // 1. is more complex
    // 2. *might* compete with some of the exponent code for integer slot
    // 3. mul is potentially lower throughput than madd
    // 4. Doesn't require load slots for the constant (low impact imo)
    // will just have to benchmark both

    for cl in &mut cleaned {
        let mul_1_10000 = _mm_setr_epi16(10000, 1, 10000, 1, 10000, 1, 10000, 1);
        *cl = _mm_madd_epi16(*cl, mul_1_10000);
    }

    let mut u32_pairs = [0; N];
    for i in 0..N {
        u32_pairs[i] = _mm_cvtsi128_si64(cleaned[i]) as u64;
    }

    let mut output = outputs;
    for i in 0..N {
        let small_bottom = u32_pairs[i] >> 32;

        // I used to have some code here where you could statically specify
        // there were less than 8 digits, but it had almost no performance impact

        let large_half = u32_pairs[i] as u32 as u64;
        std::ptr::write(output, 100000000 * large_half + small_bottom);
        output = output.add(1);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        arch::x86_64::{
            __m128i, _mm_and_si128, _mm_loadu_si128, _mm_mask_blend_epi8, _mm_maskmoveu_si128,
            _mm_or_si128, _mm_setzero_si128, _mm_slli_epi16, _mm_slli_si128, _mm_srli_epi16,
            _mm_srli_si128, _mm_storel_epi64, _mm_unpackhi_epi8, _mm_unpacklo_epi8,
        },
        ops::{Shr, ShrAssign},
    };

    use super::*;

    #[test]
    fn test_overlapping_windows() {
        let input = "1671670171236\n1671670172236\n1671670171285\n";
        let mut iter = ChunkerIter::<14, 16, 2>::new(input.as_bytes());
        assert_eq!(
            iter.next().map(as_str),
            Some(["1671670171236\n16", "1671670172236\n16"])
        );
        assert_eq!(iter.next().map(as_str), None);

        let mut iter = iter.remainder();
        assert_eq!(
            iter.next(),
            Some("1671670171285\n".as_bytes().try_into().unwrap())
        );
        assert_eq!(iter.next(), None);
    }

    fn as_str<const R: usize, const N: usize>(res: [&[u8; R]; N]) -> [&str; N] {
        let vec: Vec<_> = res
            .into_iter()
            .map(|r| std::str::from_utf8(r).unwrap())
            .collect();
        vec.try_into().unwrap()
    }

    #[test]
    fn test_simd_parsing() {
        let inputs = [
            "0000000000001\n16".as_bytes().try_into().unwrap(),
            "0000000000002\n16".as_bytes().try_into().unwrap(),
            "0000000000003\n16".as_bytes().try_into().unwrap(),
            "0000000000004\n16".as_bytes().try_into().unwrap(),
        ];
        let mut output = Vec::with_capacity(inputs.len());
        unsafe {
            do_parse_decimals(&inputs, output.as_mut_ptr_range().end);
            output.set_len(inputs.len());
        };
        assert_eq!(output.as_slice(), &[1, 2, 3, 4]);
    }

    #[test]
    fn test_4bit_packing() {
        let a: &[u8; 16] = "1234567891234\n16".as_bytes().try_into().unwrap();

        unsafe fn as_portable_simd(x: __m128i) -> std::simd::u8x16 {
            x.into()
        }

        unsafe {
            let a: __m128i = _mm_loadu_si128(a as *const u8 as *const __m128i);
            eprintln!("a\t: {:02x}", as_portable_simd(a));

            let zero = _mm_set1_epi8(b'0' as i8);
            let a = _mm_sub_epi8(a, zero);
            eprintln!("a - 0\t: {:02x}", as_portable_simd(a));

            // zero out three bytes off the end since that's not part
            // of the number
            let and_mask = _mm_setr_epi8(
                -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0x00, 0x00, 0x00,
            );
            let a = _mm_and_si128(a, and_mask);
            eprintln!("a&mask\t: {:02x}", as_portable_simd(a));

            // shift left by 4bits so that we can do a logical OR
            // to pack together 2 bytes into 1 byte
            let b = _mm_slli_epi16(a, 4);
            eprintln!("a<<4\t: {:02x}", as_portable_simd(b));

            // shift right by 1 byte so that things line up to do
            // the OR operation
            let b = _mm_slli_si128(b, 1);
            eprintln!("b>>8\t: {:02x}", as_portable_simd(b));

            // do the OR operation such that two adjacent bytes
            // end up getting packed together into one byte
            let a = _mm_or_si128(a, b);
            eprintln!("a|b\t: {:02x}", as_portable_simd(a));

            // move the bytes around such that the lower 64bits
            // contain the number that we want
            let control = _mm_setr_epi8(12, 10, 8, 6, 4, 2, 0, -1, -1, -1, -1, -1, -1, -1, -1, -1);
            let a = _mm_shuffle_epi8(a, control);
            eprintln!("shuff\t: {:02x}", as_portable_simd(a));

            // extract the lower 64bits as a u64
            let dest = _mm_cvtsi128_si64(a);
            eprintln!("dest\t: {:#02x}", dest);
        }
        assert!(false)
    }
}

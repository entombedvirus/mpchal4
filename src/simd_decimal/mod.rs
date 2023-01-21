use std::{
    arch::x86_64::{
        __m128i, _mm_and_si128, _mm_cvtsi128_si64, _mm_loadu_si128, _mm_or_si128, _mm_packus_epi16,
        _mm_set1_epi8, _mm_setr_epi8, _mm_setzero_si128, _mm_slli_epi16, _mm_slli_si128,
        _mm_sub_epi8,
    },
    io::BufRead,
    mem::MaybeUninit,
};

const REG_BYTES: usize = 16;

pub fn parse_packed_4bit<const N: usize, const LINE_WIDTH: usize>(
    inputs: &[u8],
    outputs: &mut Vec<PackedVal>,
    file_idx: u8,
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
                file_idx,
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
                file_idx,
            );
            i += 1;
        }
        outputs.set_len(expected_results);
    }
}

unsafe fn do_parse_packed_4bit<const N: usize, const LINE_WIDTH: usize>(
    inputs: &[&[u8; REG_BYTES]; N],
    outputs: &mut [PackedVal; N],
    file_idx: u8,
) {
    let zero = _mm_set1_epi8(b'0' as i8);
    let mut cleaned = [_mm_setzero_si128(); N];
    for i in 0..N {
        let a = _mm_loadu_si128(inputs[i] as *const u8 as *const __m128i);
        cleaned[i] = _mm_sub_epi8(a, zero);
    }

    let last3_mask = _mm_setr_epi8(
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0x00, 0x00, 0x00,
    );
    for i in 0..N {
        // zero out three bytes off the end since that's not part
        // of the number
        cleaned[i] = _mm_and_si128(cleaned[i], last3_mask);
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

    let every_other_mask = _mm_setr_epi8(-1, 0, -1, 0, -1, 0, -1, 0, -1, 0, -1, 0, -1, 0, -1, 0);
    for i in 0..N {
        // zero out every other byte so that we can pack into 8bits later
        cleaned[i] = _mm_and_si128(cleaned[i], every_other_mask);
    }
    for i in 0..N {
        // pack 16bits into 8bits
        cleaned[i] = _mm_packus_epi16(cleaned[i], cleaned[i]);
    }
    for i in 0..N {
        // extract the lo 64bits and swap bytes so that the most
        // significant 4bits in the right place
        let lo = _mm_cvtsi128_si64(cleaned[i]);
        outputs[i] = PackedVal::new(lo.swap_bytes() as u64, file_idx);
    }
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

/// PackedVal encodes a value that is parsed from the input files and the index of the SortedFile
/// it was parsed from. The least significant byte contains the index (since there can never by
/// more than 20 input files the file index will never be greater than 19, which will fit in a
/// byte). The remaining 7 bytes contains each ascii byte of the original number packed into
/// 4 bits. Since the input is always 13 digits and we have 7 bytes x 2 four-bits per byte = 14
/// slots, one 4bit slot will be zero.
#[derive(Debug, PartialOrd, Ord, PartialEq, Eq, Clone, Copy)]
pub struct PackedVal(u64);
impl PackedVal {
    pub const MAX: Self = Self(u64::MAX);

    pub fn new(v: u64, idx: u8) -> Self {
        debug_assert_eq!(
            v as u8, 0,
            "least significant byte must be zero to pack file_idx in it"
        );
        Self(v | idx as u64)
    }

    #[inline]
    pub fn file_idx(&self) -> u8 {
        // large to small integer conversion truncates
        self.0 as u8
    }

    #[inline]
    pub fn inner(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use std::arch::x86_64::{
        __m128i, _mm_and_si128, _mm_lddqu_si128, _mm_or_si128, _mm_packus_epi16, _mm_slli_epi16,
        _mm_slli_si128,
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
    fn test_4bit_packing() {
        let a: &[u8; 16] = "1234567891234\n16".as_bytes().try_into().unwrap();

        unsafe fn as_portable_simd(x: __m128i) -> std::simd::u8x16 {
            x.into()
        }

        unsafe {
            let a: __m128i = _mm_lddqu_si128(a as *const u8 as *const __m128i);
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

            let and_mask = _mm_setr_epi8(-1, 0, -1, 0, -1, 0, -1, 0, -1, 0, -1, 0, -1, 0, -1, 0);
            let a = _mm_and_si128(a, and_mask);
            eprintln!("a&mask\t: {:02x}", as_portable_simd(a));
            let b = _mm_packus_epi16(a, a);
            eprintln!("pack\t: {:02x}", as_portable_simd(b));

            let b = _mm_cvtsi128_si64(b).swap_bytes() as u64;
            eprintln!("b\t: {:#02x}", b);

            let packed = PackedVal::new(b, 0xCA);
            assert_eq!(0x01_23_45_67_89_12_34_CA, packed.inner());
            assert_eq!(0xCA, packed.file_idx());
        }
    }
}

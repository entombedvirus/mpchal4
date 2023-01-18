#![feature(array_windows)]
#![feature(array_chunks)]
#![feature(iter_array_chunks)]
#![feature(ptr_sub_ptr)]
#![feature(maybe_uninit_uninit_array)]
#![feature(maybe_uninit_array_assume_init)]
#![feature(portable_simd)]
#![feature(stdsimd_internal)]
#![feature(stdsimd)]
use std::{
    arch::x86_64::{
        _mm256_add_epi64, _mm256_cmplt_epu64_mask, _mm256_extracti128_si256, _mm256_load_si256,
        _mm256_loadu_si256, _mm256_mask_mov_epi64, _mm256_min_epu64, _mm256_permute4x64_epi64,
        _mm256_set1_epi64x, _mm256_set_epi64x, _mm256_setr_epi64x, _mm_add_epi64,
        _mm_cmplt_epu64_mask, _mm_load_si128, _mm_mask_mov_epi64, _mm_set1_epi64x, _mm_set_epi8,
        _mm_setr_epi8,
    },
    env, io,
    simd::{u64x2, u64x4, usizex2, usizex4, Simd, SimdOrd},
};

use iodirect::{output_file::OutputFile, sorted_file::SortedFile, ALIGN, LINE_WIDTH_INCL_NEWLINE};

mod iodirect;
mod simd_decimal;

// Flagged as dead code unfortunately
#[allow(dead_code)]
const fn check_consts() {
    assert!(
        ALIGN >= LINE_WIDTH_INCL_NEWLINE,
        "align size has to be atleast as big as one line to deal with parsing partial lines"
    )
}

const _: () = check_consts();

fn main() {
    let mut input_files: Vec<_> = env::args()
        .skip(1)
        .map(|input_file| SortedFile::new(&input_file))
        .collect();

    // provide default inputs to make running profiler easier
    if input_files.is_empty() {
        for pat in ["2", "4", "8", "10", "20", "40"] {
            let path = format!("files/{pat}m.txt");
            input_files.push(SortedFile::new(&path));
        }
    }

    let mut expected_file_size = 0;
    for file in &input_files {
        expected_file_size += file.file_size;
    }

    let mut output = OutputFile::new("result.txt", expected_file_size as usize);
    let mut wr = SortingWriter::new(input_files);
    wr.write_to(&mut output).unwrap();
}

struct SortingWriter {
    input_files: Vec<SortedFile>,
    // a parallal vec to input_files that contains the top value from each SortedFile. Used to
    // speed up comparisons to find the min value.
    min_values: Vec<Simd<u64, { Self::LANES }>>,
}

impl SortingWriter {
    const LANES: usize = 2;

    fn new(input_files: Vec<SortedFile>) -> Self {
        let min_values = input_files
            .chunks(Self::LANES)
            .map(|sfs| {
                let mut arr = [u64::MAX; Self::LANES];
                for (i, sf) in sfs.iter().enumerate() {
                    arr[i] = *sf.peek().unwrap_or(&u64::MAX);
                }
                Simd::from_array(arr)
            })
            .collect();
        Self {
            input_files,
            min_values,
        }
    }

    fn write_to(&mut self, dest: &mut OutputFile) -> io::Result<()> {
        loop {
            let Some(min_idx) = self.argmin2() else { return Ok(()) };

            let min_sf = &mut self.input_files[min_idx];
            let Some(line) = min_sf.peek_bytes() else { return Ok(()) };
            dest.write_bytes(line)?;
            min_sf.next();
            // self.min_values[min_idx] = *min_sf.peek().unwrap_or(&u64::MAX);
            let new_val = *min_sf.peek().unwrap_or(&u64::MAX);
            self.update_min_value_at(min_idx, new_val);
        }
    }

    #[inline(never)]
    fn find_min_idx(&self) -> Option<usize> {
        let min_values = self
            .min_values
            .iter()
            .fold(Simd::splat(u64::MAX), |acc, &x| acc.simd_min(x))
            .to_array();
        let min_value = min_values.into_iter().min().unwrap();
        self.input_files
            .iter()
            .position(|sf| sf.peek() == Some(&min_value))
    }

    // See: https://en.algorithmica.org/hpc/algorithms/argmin/
    fn argmin2(&self) -> Option<usize> {
        unsafe {
            let lane_width = _mm_set1_epi64x(Self::LANES as i64);
            let mut batch_start_addr = self.min_values.as_ptr() as *const _;

            let mut min_values = _mm_load_si128(batch_start_addr);
            let mut min_indices = _mm_setr_epi8(0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0);
            let mut cur_indices = min_indices;
            batch_start_addr = batch_start_addr.add(1);

            // find lane-wise min values and their indices
            for _ in 1..self.min_values.len() {
                let cur_values = _mm_load_si128(batch_start_addr);
                cur_indices = _mm_add_epi64(cur_indices, lane_width);

                let mask = _mm_cmplt_epu64_mask(cur_values, min_values);
                min_values = _mm_mask_mov_epi64(min_values, mask, cur_values);
                min_indices = _mm_mask_mov_epi64(min_indices, mask, cur_indices);

                batch_start_addr = batch_start_addr.add(1);
            }

            let min_values = u64x2::from(min_values);
            let indices = usizex2::from(min_indices);
            if min_values[0] < min_values[1] {
                Some(indices[0])
            } else {
                Some(indices[1])
            }
            // indices
            //     .as_array()
            //     .iter()
            //     .find(|idx| min_values[**idx] == *min)
            //     .copied()

            // // compare the top two mins to the bottom two in the 64x4 register
            // let min_lo = _mm256_extracti128_si256(min_values, 0);
            // let min_idx_lo = _mm256_extracti128_si256(indices, 0);
            // let min_hi = _mm256_extracti128_si256(min_values, 1);
            // let min_idx_hi = _mm256_extracti128_si256(indices, 1);

            // let mask = _mm_cmplt_epu64_mask(min_lo, min_hi);
            // let mins: u64x2 = _mm_mask_mov_epi64(min_hi, mask, min_lo).into();
            // let min_idxs: u64x2 = _mm_mask_mov_epi64(min_idx_hi, mask, min_idx_lo).into();

            // if mins[0] < mins[1] {
            //     Some(min_idxs[0] as usize)
            // } else {
            //     Some(min_idxs[1] as usize)
            // }
        }
    }

    fn argmin(&self) -> Option<usize> {
        let mut min_val = &u64::MAX;
        let mut idx = 0;
        for (batch_idx, batch) in self.min_values.iter().enumerate() {
            let batch_min = batch.as_array().iter().min().unwrap();
            if batch_min < min_val {
                min_val = batch_min;
                idx = batch_idx;
            }
        }
        self.min_values.get(idx).map(|batch| {
            let sub_idx = batch.as_array().iter().position(|x| x == min_val).unwrap();
            idx * Self::LANES + sub_idx
        })
    }

    fn update_min_value_at(&mut self, idx: usize, new_val: u64) {
        self.min_values[idx / Self::LANES][idx % Self::LANES] = new_val;
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{BufRead, BufReader},
    };

    use super::*;

    const FILE: &str = "files/2m.txt";

    #[test]
    fn test_sorted_file() {
        let mut sf = SortedFile::new(FILE);
        assert_eq!(Some(&0x167167017123600), sf.peek());
        sf.next();
        assert_eq!(Some(&0x167167017123600), sf.peek());
    }

    fn get_4bit_compressed(x: u64) -> u64 {
        let mut as_str = x.to_string();
        as_str += "00";
        u64::from_str_radix(&as_str, 16).unwrap()
    }

    #[test]
    fn test_whole_file() {
        let mut lines = stdlib_solution_iter(&[FILE]);

        let mut sf = SortedFile::new(FILE);
        let mut n = 0;
        let mut peeked_bytes = sf.peek_bytes().cloned();
        while let Some(&actual) = sf.peek() {
            let expected = lines.next().unwrap();
            assert_eq!(get_4bit_compressed(expected), actual, "line_idx: #{n}");
            assert_eq!(
                Ok(format!("{}\n", expected)),
                String::from_utf8(peeked_bytes.unwrap().to_vec()),
                "line_idx: #{n}"
            );
            sf.next();
            peeked_bytes = sf.peek_bytes().cloned();
            n += 1;
        }
        assert_eq!(2_000_000, n);
    }

    #[test]
    fn test_two_files() {
        let inputs = ["files/2m.txt", "files/4m.txt"];
        let mut temp_file = std::env::temp_dir();
        temp_file.push("mpchal4.tmp.txt");

        {
            let sorted_files: Vec<_> = inputs.iter().copied().map(SortedFile::new).collect();
            let expected_file_size: usize =
                sorted_files.iter().map(|sf| sf.file_size as usize).sum();
            let mut wr = SortingWriter::new(sorted_files);
            let mut output = {
                OutputFile::new(
                    temp_file.as_path().to_str().unwrap(),
                    expected_file_size as usize,
                )
            };
            wr.write_to(&mut output).unwrap();
        }

        let mut expected = stdlib_solution_iter(&inputs);
        let actual = BufReader::new(fs::File::open(&temp_file).unwrap()).lines();
        let mut nr = 0;
        for line in actual {
            assert_eq!(
                expected.next().unwrap().to_string(),
                line.unwrap(),
                "line_idx: {nr}"
            );
            nr += 1;
        }
        assert_eq!(
            expected.next(),
            None,
            "our solution did not return all values"
        );
    }

    fn stdlib_solution_iter(file_names: &[&str]) -> impl Iterator<Item = u64> {
        let mut res = Vec::new();
        for f in file_names {
            let lines = BufReader::new(fs::File::open(f).unwrap()).lines();
            let lines = lines.map(|x| x.unwrap().parse::<u64>().unwrap());
            res.extend(lines);
        }
        res.sort();
        res.into_iter()
    }
}

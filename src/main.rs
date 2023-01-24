#![feature(array_windows)]
#![feature(array_chunks)]
#![feature(iter_array_chunks)]
#![feature(ptr_sub_ptr)]
#![feature(maybe_uninit_uninit_array)]
#![feature(maybe_uninit_array_assume_init)]
#![feature(portable_simd)]
#![feature(stdsimd_internal)]
#![feature(stdsimd)]
use std::{env, io};

use iodirect::{output_file::OutputFile, sorted_file::SortedFile, ALIGN, LINE_WIDTH_INCL_NEWLINE};

mod iodirect;
mod simd_decimal;

// The main strategies involved in this solution are:
//
// 1. Skipping linux pagecache by using O_DIRECT when reading inputs and not skipping it when writing
// 2. Using SIMD to parse 4bit packed numbers for improve throughput
// 3. Using a single IO thread to do the actual blocking IO while the main thread does everything
//    else.
//
// When reading the input files, we can minimize the amount of system cpu by using O_DIRECT mode
// and skipping the page cache. Since we can get away with reading the inputs only once, the CPU
// time spent by the kernel maintaining the page cache is not worth it. However, the opposite is
// true when we are writing out the merged file: only writing to the page cache allows the program
// to only block for the time necessary to write to page cache and not the actual SSD. This allows
// us to have maximum ROI in terms of kernel / system cpu.
//
// After reading the inputs, in order to figure out which number to write next, we have to compare
// the top value from each input to find the minimum. Since all numbers are guaranteed to be 13
// digits, we could simply compare the ascii bytes without parsing it into a binary number. However,
// doing memcmp on 13 bytes repeatedly takes more cpu than parsing the ascii number into a binary
// number once and then using that number for comparisons. Using a data structure like a min-heap
// turned out to be more expensive when compared to doing a linear search for the new minimum for
// small number of input files, which is guaranteed to not exceed 20.
//
// The cpu cycles it takes to parse an ascii number to a binary u64 can be improved by using SIMD
// instructions: i.e instead of parsing each ascii digit individually, load all 13 digits into a
// single vector register and apply each step involved in parsing across each digit in the same CPU
// clock cycle. This solution takes this approach a bit further by exploiting the fact that we
// don't need to do any arithmetic on this parsed number; all we need is to be able to compare them
// to figure out the minimum. So is there a way to parse the numbers in such a way that we don't
// spend as cpu parsing them, but still gives the same result when comparing?
//
// Turns out the answer is yes. It works by compressing each ascii digit, which are 8 bits each, to
// take only 4 bits and the reversing the order such that the most significant digit ends up being
// in the most significant position. For example:
// - start with the ascii number "1234"
// - represented as array of bytes, this is equivalent to [0x31, 0x32, 0x33, 0x34]
// - subtract ascii '0' from each byte so that we get:    [0x1, 0x2, 0x3, 0x4]
// - since the max possible legal value is 0x9, which fits within 4 bits, we can pack
//   two digits into every byte: [0x12, 0x34]
// - since "12" appears in the most significant positions in the original number, we need to
//   reverse the order in the byte array so that when comparing numbers, "1234" will be smaller
//   than "1235": [0x34, 0x12]
//
//   Similar to normal parsing of ascii numbers to binary numbers, this 4bit packed parsing can
//   also be done via SIMD instructions. See: do_parse_packed_4bit in the simd_decimal module to
//   see the full implementation.
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

struct SortingWriter(Vec<SortedFile>);

impl SortingWriter {
    fn new(sfs: Vec<SortedFile>) -> Self {
        Self(sfs)
    }

    fn write_to(&mut self, dest: &mut OutputFile) -> io::Result<()> {
        loop {
            let Some(min_sf) = self
                .0
                .iter_mut()
                .min_by_key(|sf| *sf.peek().unwrap_or(&u64::MAX)) else { return Ok(()) };

            match min_sf.peek_bytes() {
                None => break Ok(()),
                Some(line) => {
                    dest.write_bytes(line)?;
                    min_sf.next();
                }
            }
        }
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

// Flagged as dead code unfortunately
#[allow(dead_code)]
const fn check_consts() {
    assert!(
        ALIGN >= LINE_WIDTH_INCL_NEWLINE,
        "align size has to be atleast as big as one line to deal with parsing partial lines"
    )
}

const _: () = check_consts();

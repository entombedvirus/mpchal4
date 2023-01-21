#![feature(array_windows)]
#![feature(array_chunks)]
#![feature(iter_array_chunks)]
#![feature(ptr_sub_ptr)]
#![feature(maybe_uninit_uninit_array)]
#![feature(maybe_uninit_array_assume_init)]
#![feature(portable_simd)]
#![feature(stdsimd_internal)]
#![feature(stdsimd)]
#![feature(iterator_try_reduce)]
use std::{env, io};

use iodirect::{output_file::OutputFile, sorted_file::SortedFile, ALIGN, LINE_WIDTH_INCL_NEWLINE};
use simd_decimal::PackedVal;

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
        .enumerate()
        .map(|(idx, input_file)| SortedFile::new(&input_file, idx as u8))
        .collect();

    // provide default inputs to make running profiler easier
    if input_files.is_empty() {
        for (idx, pat) in ["2", "4", "8", "10", "20", "40"].iter().enumerate() {
            let path = format!("files/{pat}m.txt");
            input_files.push(SortedFile::new(&path, idx as u8));
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
            let seq = self.get_write_sequence();
            if seq.is_empty() {
                return Ok(());
            }

            for idx in seq {
                let min_sf = &mut self.0[idx as usize];
                let line = min_sf.peek_bytes().unwrap();
                dest.write_bytes(line)?;
                min_sf.next();
            }
        }
    }

    fn get_write_sequence(&self) -> Vec<u8> {
        let mut ret = Vec::new();

        // find all the files with values remaining that we can step thru one value at a time
        // finding the min each round
        let mut iters: Vec<_> = self
            .0
            .iter()
            .filter(|sf| sf.peek().is_some())
            .map(|sf| sf.parsed_values().iter().peekable())
            .collect();

        loop {
            let min_val = iters
                .iter_mut()
                .try_reduce(|acc, e| {
                    // if any iter has None when peeked, that means we need to go back to disk to
                    // fetch a new batch. Since we don't know whether the item that will be read
                    // will be new min, bail out early
                    let acc_top = acc.peek()?;
                    let e_top = e.peek()?;
                    Some(if acc_top < e_top { acc } else { e })
                })
                .flatten()
                .and_then(|min_iter| min_iter.next());

            match min_val {
                None => {
                    // self.0 is empty or all the iterators ran out
                    break;
                }
                Some(pval) => {
                    ret.push(pval.file_idx());
                }
            }
        }
        ret
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{BufRead, BufReader},
    };

    use crate::simd_decimal::PackedVal;

    use super::*;

    const FILE: &str = "files/2m.txt";

    #[test]
    fn test_sorted_file() {
        let mut sf = SortedFile::new(FILE, 0);
        assert_eq!(Some(0x167167017123600), sf.peek().map(PackedVal::inner));
        sf.next();
        assert_eq!(Some(0x167167017123600), sf.peek().map(PackedVal::inner));
    }

    fn get_4bit_compressed(x: u64, file_idx: u8) -> PackedVal {
        let mut as_str = x.to_string();
        as_str += "00";
        let val = u64::from_str_radix(&as_str, 16).unwrap();
        PackedVal::new(val, file_idx)
    }

    #[test]
    fn test_whole_file() {
        let mut lines = stdlib_solution_iter(&[FILE]);
        let file_idx = 19;

        let mut sf = SortedFile::new(FILE, file_idx);
        let mut n = 0;
        let mut peeked_bytes = sf.peek_bytes().cloned();
        while let Some(&actual) = sf.peek() {
            let expected = lines.next().unwrap();
            assert_eq!(
                get_4bit_compressed(expected, file_idx),
                actual,
                "line_idx: #{n}"
            );
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
            let sorted_files: Vec<_> = inputs
                .iter()
                .copied()
                .enumerate()
                .map(|(idx, f)| SortedFile::new(f, idx as u8))
                .collect();
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

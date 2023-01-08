#![feature(array_windows)]
#![feature(array_chunks)]
#![feature(iter_array_chunks)]
#![feature(ptr_sub_ptr)]
#![feature(maybe_uninit_uninit_array)]
#![feature(maybe_uninit_array_assume_init)]
use std::env;

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
    let mut input: Vec<_> = env::args()
        .skip(1)
        .map(|input_file| SortedFile::new(&input_file))
        .collect();

    // provide default inputs to make running profiler easier
    if input.is_empty() {
        for pat in ["2", "4", "8", "10", "20", "40"] {
            let path = format!("files/{pat}m.txt");
            input.push(SortedFile::new(&path));
        }
    }

    let mut expected_file_size = 0;
    for file in &input {
        expected_file_size += file.file_size;
    }

    let mut output = OutputFile::new("result.txt", expected_file_size as usize);

    while let Some(idx) = find_min_idx(&mut input) {
        let sorted_file = &mut input[idx];
        if let Some(v) = sorted_file.next() {
            output.write_u64(v).expect("output.write_u64 failed");
        } else {
            input.swap_remove(idx);
        }
    }
}

fn find_min_idx(files: &mut [SortedFile]) -> Option<usize> {
    let mut min_idx: usize = 0;
    let mut min: u64 = u64::MAX;
    if files.is_empty() {
        return None;
    }
    for (idx, sf) in files.iter_mut().enumerate() {
        match sf.peek() {
            None => return Some(idx),
            Some(val) => {
                if val < min {
                    min = val;
                    min_idx = idx;
                }
            }
        }
    }
    Some(min_idx)
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
        assert_eq!(Some(1671670171236_u64), sf.next());
        assert_eq!(Some(1671670171236_u64), sf.next());
    }

    #[test]
    fn test_whole_file() {
        let mut lines = stdlib_solution_iter(&[FILE]);

        let mut sf = SortedFile::new(FILE);
        let mut n = 0;
        while let Some(actual) = sf.next() {
            let expected: u64 = lines.next().unwrap();
            assert_eq!(expected, actual, "line_idx: #{n}");
            n += 1;
        }
        assert_eq!(2_000_000, n);
    }

    #[test]
    fn test_two_files() {
        let inputs = ["files/2m.txt", "files/4m.txt"];
        let mut expected = stdlib_solution_iter(&inputs);
        let mut sorted_files: Vec<_> = inputs.iter().copied().map(SortedFile::new).collect();

        let mut nr = 0;
        loop {
            match find_min_idx(&mut sorted_files) {
                None => {
                    assert_eq!(None, expected.next(), "our solution exited too early");
                    break;
                }
                Some(idx) => {
                    match (&mut sorted_files[idx]).next() {
                        None => {
                            // this file is exhausted
                            sorted_files.swap_remove(idx);
                            continue;
                        }
                        Some(actual) => {
                            assert_eq!(expected.next().unwrap(), actual, "line_idx: {nr}");
                        }
                    }
                }
            }
            nr += 1;
        }
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

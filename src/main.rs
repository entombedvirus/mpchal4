#![feature(array_windows)]
#![feature(array_chunks)]
#![feature(iter_array_chunks)]
#![feature(ptr_sub_ptr)]
#![feature(maybe_uninit_uninit_array)]
#![feature(maybe_uninit_array_assume_init)]
use std::{
    collections::{binary_heap::PeekMut, BinaryHeap},
    env,
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
    write_all(LinearSearchIter::new(input_files), &mut output);
}

fn write_all<I: Iterator<Item = u64>>(iter: I, dest: &mut OutputFile) {
    for v in iter {
        dest.write_u64(v).expect("output.write_u64 failed");
    }
}

struct LinearSearchIter(Vec<SortedFile>);

impl LinearSearchIter {
    fn new(sfs: Vec<SortedFile>) -> Self {
        Self(sfs)
    }

    fn find_min_idx(&self) -> Option<usize> {
        let files = &self.0;
        let mut min_idx: usize = 0;
        let mut min: u64 = u64::MAX;
        if files.is_empty() {
            return None;
        }
        for (idx, sf) in files.iter().enumerate() {
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
}

impl Iterator for LinearSearchIter {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.find_min_idx() {
                Some(idx) => {
                    let ret = self.0[idx].next();
                    if ret.is_none() {
                        self.0.swap_remove(idx);
                        continue;
                    } else {
                        break ret;
                    }
                }
                None => break None,
            }
        }
    }
}

// this turned out to be be more cpu intensive that linear search for
// small number of input files. Since the problem statement limits the
// max number of inputs to 20, this is not used.
//
// Leaving it as a reference and for benchmarking.
struct HeapSearchIter(BinaryHeap<SortedFile>);

impl HeapSearchIter {
    #[allow(dead_code)]
    fn new(sfs: Vec<SortedFile>) -> Self {
        Self(sfs.into())
    }
}

impl Iterator for HeapSearchIter {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        let heap = &mut self.0;
        loop {
            let mut top = heap.peek_mut()?;
            match top.next() {
                None => {
                    PeekMut::pop(top);
                    continue;
                }
                val => {
                    if top.peek().is_none() {
                        PeekMut::pop(top);
                    }
                    return val;
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
        let sorted_files: Vec<_> = inputs.iter().copied().map(SortedFile::new).collect();
        let iter = HeapSearchIter::new(sorted_files);

        let mut nr = 0;
        for actual in iter {
            assert_eq!(expected.next().unwrap(), actual, "line_idx: {nr}");
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

#![feature(array_windows)]
#![feature(array_chunks)]
#![feature(iter_array_chunks)]
#![feature(ptr_sub_ptr)]
#![feature(maybe_uninit_uninit_array)]
#![feature(maybe_uninit_array_assume_init)]
#![feature(portable_simd)]
use std::{env, io};

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

struct SortingWriter(Vec<SortedFile>);

impl SortingWriter {
    fn new(sfs: Vec<SortedFile>) -> Self {
        Self(sfs)
    }

    fn find_min_idx(&self) -> Option<usize> {
        let files = &self.0;
        let mut min_idx: usize = 0;
        let mut min = u128::MAX;
        if files.is_empty() {
            return None;
        }
        for (idx, sf) in files.iter().enumerate() {
            match sf.peek() {
                None => return Some(idx),
                Some(val) => {
                    if val < &min {
                        min = *val;
                        min_idx = idx;
                    }
                }
            }
        }
        Some(min_idx)
    }

    fn write_to(&mut self, dest: &mut OutputFile) -> io::Result<()> {
        loop {
            match self.find_min_idx() {
                Some(idx) => {
                    let sf = &mut self.0[idx];
                    if let Some(line) = sf.peek_bytes() {
                        dest.write_bytes(line)?;
                        sf.next();
                    } else {
                        self.0.swap_remove(idx);
                    }
                }
                None => break Ok(()),
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
        assert_eq!(Some(1671670171236), sf.peek());
        sf.next();
        assert_eq!(Some(1671670171236), sf.peek());
    }

    #[test]
    fn test_whole_file() {
        let mut lines = stdlib_solution_iter(&[FILE]);

        let mut sf = SortedFile::new(FILE);
        let mut n = 0;
        let mut peeked_bytes = sf.peek_bytes().cloned();
        while let Some(actual) = sf.peek() {
            let expected = lines.next().unwrap();
            assert_eq!(expected, actual, "line_idx: #{n}");
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

    fn stdlib_solution_iter(file_names: &[&str]) -> impl Iterator<Item = u128> {
        let mut res = Vec::new();
        for f in file_names {
            let lines = BufReader::new(fs::File::open(f).unwrap()).lines();
            let lines = lines.map(|x| x.unwrap().parse::<u128>().unwrap());
            res.extend(lines);
        }
        res.sort();
        res.into_iter()
    }
}

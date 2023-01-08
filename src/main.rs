#![feature(array_windows)]
#![feature(array_chunks)]
#![feature(iter_array_chunks)]
#![feature(ptr_sub_ptr)]
use std::{
    env, fs,
    io::{self, ErrorKind, Read},
    sync::mpsc::RecvError,
};

use rustix::fs::{MetadataExt, OpenOptionsExt};
mod iodirect;
mod simd_decimal;

// Flagged as dead code unfortunately
#[allow(dead_code)]
const fn check_consts() {
    assert!(
        iodirect::ALIGN >= LINE_WIDTH_INCL_NEWLINE,
        "align size has to be atleast as big as one line to deal with parsing partial lines"
    )
}

const _: () = check_consts();

fn main() {
    let mut input: Vec<_> = env::args()
        .skip(1)
        .map(|input_file| SortedFile::new(&input_file))
        .collect();

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

    let mut output = iodirect::File::new("result.txt", expected_file_size as usize);

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

#[derive(Debug)]
struct SortedFile {
    file_size: u64,

    cur_buf: Option<ReadBuffer>,
    worker: Option<std::thread::JoinHandle<()>>,
    io: Option<std::sync::mpsc::Receiver<ReadBuffer>>,
}

#[derive(Debug)]
struct ReadBuffer {
    parsed_lines: Vec<u64>,
    parsed_line_pos: usize,

    aligned_buf: Box<[u8]>,
    pos: usize,
    filled: usize,
}

impl ReadBuffer {
    pub fn new(file_size: usize) -> Self {
        let aligned_buf = unsafe {
            const SZ: usize = 1 << 20;

            // leave ALIGN size bytes in the beginning to deal with
            // partial lines while parsing
            let alloc_size = iodirect::ALIGN + SZ;
            let layout = std::alloc::Layout::from_size_align(alloc_size, iodirect::ALIGN).unwrap();
            let ptr = std::alloc::alloc_zeroed(layout);
            let slice = std::slice::from_raw_parts_mut(ptr, alloc_size);
            Box::from_raw(slice)
        };

        Self {
            parsed_lines: Vec::with_capacity(file_size / LINE_WIDTH_INCL_NEWLINE),
            parsed_line_pos: 0,

            aligned_buf,
            pos: 0,
            filled: 0,
        }
    }

    pub fn fill_buf<R: Read>(&mut self, mut reader: R) -> io::Result<usize> {
        self.pos = iodirect::ALIGN;
        self.filled = self.pos;
        let mut buf = &mut self.aligned_buf[self.pos..];
        while self.filled - self.pos < LINE_WIDTH_INCL_NEWLINE {
            match reader.read(buf) {
                Ok(0) => break, // eof
                Ok(non_zero) => {
                    self.filled += non_zero;
                    buf = &mut buf[non_zero..];
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        let avail = self.filled - self.pos;
        if avail > 0 && avail < LINE_WIDTH_INCL_NEWLINE {
            // will only happen once if the last line is missing
            // newline
            self.aligned_buf[self.filled] = b'\n';
            self.filled += 1;
        }
        Ok(avail)
    }

    fn parse_buf(&mut self, partial_line: &mut Vec<u8>) {
        let dst = &mut self.aligned_buf[self.pos - partial_line.len()..self.pos];
        dst.copy_from_slice(partial_line);
        self.pos -= partial_line.len();

        partial_line.clear();
        let buf = &self.aligned_buf[self.pos..self.filled];
        let n = buf.len() % LINE_WIDTH_INCL_NEWLINE;
        partial_line.extend_from_slice(&self.aligned_buf[self.filled - n..self.filled]);

        let num_complete_lines = buf.len() / LINE_WIDTH_INCL_NEWLINE;
        simd_decimal::parse_decimals::<4, LINE_WIDTH_INCL_NEWLINE>(
            &buf[..num_complete_lines * LINE_WIDTH_INCL_NEWLINE],
            &mut self.parsed_lines,
        );
    }
}
const LINE_WIDTH_INCL_NEWLINE: usize = 14;

impl SortedFile {
    fn new(file_path: &str) -> Self {
        let reader = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(file_path)
            .expect("failed to open input");
        let file_size = reader.metadata().unwrap().size();

        let (tx, rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let mut reader = reader;
            let mut partial_line = Vec::with_capacity(LINE_WIDTH_INCL_NEWLINE);

            loop {
                let mut new_buf = ReadBuffer::new(file_size as usize);
                let nr = new_buf
                    .fill_buf(&mut reader)
                    .expect("sorted_file: worker: failed to read");
                if nr == 0 {
                    // eof
                    return;
                }
                new_buf.parse_buf(&mut partial_line);
                match tx.send(new_buf) {
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        });

        Self {
            file_size,

            cur_buf: None,
            worker: Some(worker),
            io: Some(rx),
        }
    }

    pub fn peek(&mut self) -> Option<u64> {
        loop {
            if let Some(cur_buf) = self.cur_buf.as_ref() {
                if let Some(val) = cur_buf.parsed_lines.get(cur_buf.parsed_line_pos) {
                    break Some(*val);
                }
            }

            match self.io.as_ref().unwrap().recv() {
                Ok(new_buf) => {
                    self.cur_buf = Some(new_buf);
                    continue;
                }
                Err(RecvError) => break None,
            }
        }
    }

    pub fn next(&mut self) -> Option<u64> {
        let ret = self.peek();
        if ret.is_some() {
            self.cur_buf.as_mut().unwrap().parsed_line_pos += 1;
        }
        ret
    }
}

impl Drop for SortedFile {
    fn drop(&mut self) {
        // signal worker to shutdown
        let _ = self.io.take();

        // wait for worker
        self.worker.take().unwrap().join().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader};

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

use std::{
    env, fs,
    io::{ErrorKind, Read},
};

use rustix::fs::{MetadataExt, OpenOptionsExt};
mod iodirect;

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
        if let Some(line) = sorted_file.next() {
            output.write_bytes(line).expect("output.write_bytes failed");
        } else {
            input.swap_remove(idx);
        }
    }
}

fn find_min_idx(files: &mut [SortedFile]) -> Option<usize> {
    files
        .iter_mut()
        .enumerate()
        .map(|(idx, sf)| (idx, sf.peek()))
        .min_by_key(|(_, val)| *val)
        .map(|(idx, _)| idx)
}

#[derive(Debug)]
struct SortedFile {
    file_size: u64,

    parsed_lines: Vec<u64>,
    parsed_line_pos: usize,
    partial_line_bytes: usize,

    reader: fs::File,
    aligned_buf: Box<[u8]>,
    pos: usize,
    filled: usize,
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

        let mut ret = Self {
            file_size,

            parsed_lines: Vec::with_capacity(file_size as usize / LINE_WIDTH_INCL_NEWLINE),
            parsed_line_pos: 0,
            partial_line_bytes: 0,

            reader,
            aligned_buf,
            pos: 0,
            filled: 0,
        };
        ret.fill_parsed_lines();
        ret
    }

    pub fn peek(&mut self) -> Option<u64> {
        match self.parsed_lines.get(self.parsed_line_pos) {
            Some(val) => Some(*val),
            None => {
                self.fill_parsed_lines();
                self.parsed_lines.get(self.parsed_line_pos).copied()
            }
        }
    }

    pub fn next(&mut self) -> Option<&[u8]> {
        self.fill_parsed_lines();
        match self.parsed_lines.get(self.parsed_line_pos) {
            None => None,
            Some(_) => {
                self.parsed_line_pos += 1;
                let range = self.pos..self.pos + LINE_WIDTH_INCL_NEWLINE;
                self.pos += LINE_WIDTH_INCL_NEWLINE;
                self.aligned_buf.get(range)
            }
        }
    }

    fn fill_parsed_lines(&mut self) {
        if self.parsed_line_pos < self.parsed_lines.len() {
            return;
        }
        assert_eq!(self.parsed_line_pos, self.parsed_lines.len());

        self.parsed_line_pos = 0;
        self.parsed_lines.clear();

        self.pos = iodirect::ALIGN;
        self.filled = self.pos;

        if self.partial_line_bytes > 0 {
            self.pos -= self.partial_line_bytes;
            self.aligned_buf
                .copy_within(0..self.partial_line_bytes, self.pos);
            self.partial_line_bytes = 0;
        }

        self.fill_buf();

        let buf = &self.aligned_buf[self.pos..self.filled];
        let lines = buf.chunks_exact(LINE_WIDTH_INCL_NEWLINE);
        let partial_line = lines.remainder();
        self.partial_line_bytes = partial_line.len();

        for line in lines {
            let parsed_line = parse_num_with_newline(line);
            self.parsed_lines.push(parsed_line);
        }

        let n = self.partial_line_bytes;
        // save the partial line at beginning so that we can copy
        // it to the right place next time
        self.aligned_buf
            .copy_within(self.filled - n..self.filled, 0);
    }

    fn fill_buf(&mut self) {
        let mut buf = &mut self.aligned_buf[iodirect::ALIGN..];
        while self.filled - self.pos < LINE_WIDTH_INCL_NEWLINE {
            match self.reader.read(buf) {
                Ok(0) => break, // eof
                Ok(non_zero) => {
                    self.filled += non_zero;
                    buf = &mut buf[non_zero..];
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => panic!("fill_parsed_lines: read from file failed: {e})"),
            }
        }
        let avail = self.filled - self.pos;
        if avail > 0 && avail < LINE_WIDTH_INCL_NEWLINE {
            // will only happen once if the last line is missing
            // newline
            self.aligned_buf[self.filled] = b'\n';
            self.filled += 1;
        }
    }
}

fn parse_num_with_newline(digits: &[u8]) -> u64 {
    // ignore empty and just newline char
    assert!(
        digits.len() > 1,
        "expecting at least one digit plus newline"
    );

    let mut res: u64 = 0;
    for &c in &digits[..digits.len() - 1] {
        res *= 10;
        let digit = (c as u64) - '0' as u64;
        res += digit;
    }
    res
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader};

    use super::*;

    const FILE: &str = "files/2m.txt";

    #[test]
    fn test_sorted_file() {
        let mut sf = SortedFile::new(FILE);
        assert_eq!(Some("1671670171236\n".as_bytes()), sf.next());
        assert_eq!(Some("1671670171236\n".as_bytes()), sf.next());
    }

    #[test]
    fn test_whole_file() {
        let of = fs::File::open(FILE).unwrap();
        let mut lines = BufReader::new(of).lines();

        let mut sf = SortedFile::new(FILE);
        let mut n = 0;
        while let Some(bs) = sf.next() {
            let mut with_nl = lines.next().unwrap().unwrap();
            with_nl.push('\n');
            let as_str = std::str::from_utf8(bs).unwrap();
            assert_eq!(&with_nl, as_str, "line_idx: #{n}");
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
                            let actual_str = std::str::from_utf8(actual).expect("not valid utf8");
                            let mut expected_str = expected
                                .next()
                                .expect("stdlib solution exhausted too early")
                                .to_string();
                            expected_str.push('\n');
                            assert_eq!(expected_str, actual_str, "line_idx: {nr}");
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

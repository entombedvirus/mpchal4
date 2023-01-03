use std::{env, fs, io::Read};

use rustix::fs::{MetadataExt, OpenOptionsExt};
mod iodirect;

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

    while let Some(idx) = find_min(&input) {
        let sorted_file = &mut input[idx];
        output
            .write_bytes(sorted_file.min_value())
            .expect("output.write_bytes failed");
        if !sorted_file.next_line() {
            // PeekMut::<'_, SortedFile>::pop(sorted_file);
            input.swap_remove(idx);
        }
    }
}

fn find_min(files: &[SortedFile]) -> Option<usize> {
    files
        .iter()
        .enumerate()
        .min_by_key(|&(_, file)| file.parsed_min_value)
        .map(|(idx, _)| idx)
}

#[derive(Debug)]
struct SortedFile {
    file_size: u64,

    newline_idx: Option<usize>,
    parsed_min_value: u64,
    partial_line: Option<Vec<u8>>,

    reader: fs::File,
    aligned_buf: Box<[u8]>,
    pos: usize,
    filled: usize,
}

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
            let layout = std::alloc::Layout::from_size_align(SZ, iodirect::ALIGN).unwrap();
            let ptr = std::alloc::alloc_zeroed(layout);
            let slice = std::slice::from_raw_parts_mut(ptr, SZ);
            Box::from_raw(slice)
        };

        let mut ret = Self {
            file_size,
            newline_idx: None,
            parsed_min_value: 0,
            partial_line: None,
            reader,
            aligned_buf,
            pos: 0,
            filled: 0,
        };

        ret.next_line();
        ret
    }

    pub fn min_value(&self) -> &[u8] {
        if let Some(line) = &self.partial_line {
            return line;
        }
        match self.newline_idx {
            Some(idx) => unsafe { self.aligned_buf.get_unchecked(self.pos..=idx) },
            None => &[],
        }
    }

    pub fn next_line(&mut self) -> bool {
        if self.partial_line.is_some() {
            self.partial_line = None;
        }
        if let Some(idx) = self.newline_idx {
            self.pos = idx + 1; // +1 to skip newline char
        }

        let found = loop {
            let avail = self.fill_buf();
            if avail == 0 {
                break false;
            }

            // perf: eliminate unnecessary bounds check
            // SAFETY: we guarantee that self.pos is always a valid index into aligned buf
            let buf = unsafe { self.aligned_buf.get_unchecked(self.pos..self.filled) };
            match memchr::memchr(b'\n', buf) {
                Some(n) => {
                    if let Some(partial_line) = &mut self.partial_line {
                        partial_line.extend_from_slice(&buf[..=n]);
                        self.newline_idx = None;
                        self.pos += n + 1;
                    } else {
                        self.newline_idx = Some(self.pos + n);
                    }
                    break true;
                }
                None => {
                    if self.partial_line.is_none() {
                        self.partial_line = Some(vec![]);
                    }
                    let partial_line = self.partial_line.as_mut().unwrap();
                    partial_line.extend_from_slice(buf);
                    self.pos += buf.len();
                    continue;
                }
            }
        };
        if !found {
            if let Some(partial_line) = self.partial_line.as_mut() {
                // last line is missing newline: normalize so that higher layers can always
                // rely on the fact that there will be a newline at the end
                partial_line.push(b'\n');
            }
        }

        self.parsed_min_value = parse_num_with_newline(self.min_value());
        found || self.partial_line.is_some()
    }

    fn fill_buf(&mut self) -> usize {
        if self.pos >= self.filled {
            debug_assert_eq!(self.pos, self.filled);
            let n = self
                .reader
                .read(&mut self.aligned_buf)
                .expect("failed to read form file");
            self.pos = 0;
            self.filled = n;
        }
        self.filled - self.pos
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
    use super::*;

    #[test]
    fn test_sorted_file() {
        const FILE: &str = "files/2m.txt";
        let mut sf = SortedFile::new(FILE);
        assert_eq!("1671670171236\n".as_bytes(), sf.min_value());

        assert_eq!(true, sf.next_line());
        assert_eq!("1671670171236\n".as_bytes(), sf.min_value());
    }
}

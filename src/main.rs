use std::{env, fs, io::Read};

use libc::memchr;
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

    while let Some(idx) = find_min_idx(&input) {
        let sorted_file = &mut input[idx];
        output
            .write_bytes(sorted_file.min_value_ascii_bytes())
            .expect("output.write_bytes failed");
        if !sorted_file.next_line() {
            input.swap_remove(idx);
        }
    }
}

fn find_min_idx(files: &[SortedFile]) -> Option<usize> {
    files
        .iter()
        .enumerate()
        .min_by_key(|&(_, file)| file.parsed_min_value)
        .map(|(idx, _)| idx)
}

#[derive(Debug, PartialEq)]
enum LineReadState {
    WaitingForScan,
    NewlineFound(usize),
    PartialLine(Vec<u8>),
}

#[derive(Debug)]
struct SortedFile {
    parsed_min_value: u64,
    file_size: u64,

    state: LineReadState,

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
            state: LineReadState::WaitingForScan,
            reader,
            parsed_min_value: 0,
            aligned_buf,
            pos: 0,
            filled: 0,
        };

        ret.next_line();
        ret
    }

    pub fn min_value_ascii_bytes(&self) -> &[u8] {
        use LineReadState::*;
        match self.state {
            WaitingForScan => &[],
            PartialLine(ref line) => line,
            NewlineFound(idx) => unsafe { self.aligned_buf.get_unchecked(self.pos..=idx) },
        }
    }

    pub fn next_line(&mut self) -> bool {
        use LineReadState::*;

        // reset previous state if necessary
        match self.state {
            PartialLine(_) => {
                self.state = WaitingForScan;
            }
            NewlineFound(idx) => {
                self.pos = idx + 1; // +1 to skip newline char
            }
            WaitingForScan => (),
        }

        let newline_found = loop {
            let avail = self.fill_buf();
            if avail == 0 {
                break false;
            }

            const LINE_WIDTH_INCL_NEWLINE: usize = 14;

            // perf: eliminate unnecessary bounds check
            // SAFETY: we guarantee that self.pos is always a valid index into aligned buf
            let buf = unsafe { self.aligned_buf.get_unchecked(self.pos..self.filled) };
            let newline_idx = match self.state {
                WaitingForScan | NewlineFound(_) if buf.len() >= LINE_WIDTH_INCL_NEWLINE => {
                    // in both of these cases, we are at a line
                    // boundary
                    Some(LINE_WIDTH_INCL_NEWLINE - 1)
                }
                _ => {
                    // we have to scan for newline since the previous iteraion
                    // read part of this line
                    memchr::memchr(b'\n', buf)
                }
            };

            match newline_idx {
                Some(n) => {
                    match self.state {
                        PartialLine(ref mut partial_line) => {
                            partial_line.extend_from_slice(&buf[..=n]);
                            self.pos += n + 1;
                        }
                        WaitingForScan | NewlineFound(_) => {
                            self.state = NewlineFound(self.pos + n);
                        }
                    }
                    break true;
                }
                None => {
                    match self.state {
                        WaitingForScan | NewlineFound(_) => self.state = PartialLine(buf.into()),
                        PartialLine(ref mut pl) => {
                            pl.extend_from_slice(buf);
                        }
                    };
                    self.pos += buf.len();
                    continue;
                }
            }
        };

        self.parsed_min_value = parse_num_with_newline(self.min_value_ascii_bytes());

        if let PartialLine(ref mut buf) = self.state {
            if !newline_found {
                // last line is missing newline: normalize so that higher layers can always
                // rely on the fact that there will be a newline at the end
                buf.push(b'\n');
            }
            true
        } else {
            newline_found
        }
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
        assert_eq!("1671670171236\n".as_bytes(), sf.min_value_ascii_bytes());

        assert_eq!(true, sf.next_line());
        assert_eq!("1671670171236\n".as_bytes(), sf.min_value_ascii_bytes());
    }
}

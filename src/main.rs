use std::{
    collections::{binary_heap::PeekMut, BinaryHeap},
    env, fs,
    io::{BufRead, BufReader, Read},
};

use rustix::fs::{MetadataExt, OpenOptionsExt};
mod iodirect;

fn main() {
    let mut input: BinaryHeap<_> = env::args()
        .skip(1)
        .map(|input_file| Box::new(SortedFile::new(&input_file)))
        .collect();

    if input.is_empty() {
        for pat in ["2", "4", "8", "10", "20", "40"] {
            let path = format!("files/{pat}m.txt");
            input.push(Box::new(SortedFile::new(&path)));
        }
    }

    let mut expected_file_size = 0;
    for file in &input {
        expected_file_size += file.file_size;
    }

    let mut output = iodirect::File::new("result.txt", expected_file_size as usize);

    while !input.is_empty() {
        let mut sorted_file = input.peek_mut().unwrap();
        output
            .write_bytes(&sorted_file.min_value)
            .expect("output.write_bytes failed");
        if !sorted_file.next_line() {
            PeekMut::<'_, Box<SortedFile>>::pop(sorted_file);
        }
    }
}

#[derive(Debug)]
struct SortedFile {
    file_size: u64,

    min_value: Vec<u8>,
    parsed_min_value: Option<u64>,

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

        let min_value = Vec::new();

        let mut ret = Self {
            file_size,
            min_value,
            parsed_min_value: None,
            reader,
            aligned_buf,
            pos: 0,
            filled: 0,
        };

        ret.next_line();
        ret
    }

    pub fn next_line(&mut self) -> bool {
        self.min_value.clear();

        let found = loop {
            let avail = self.fill_buf();
            if avail == 0 {
                break false;
            }

            // perf: eliminate unnecessary bounds check
            // SAFETY: we guarantee that self.pos is always a valid index into aligned buf
            let buf = unsafe { self.aligned_buf.get_unchecked(self.pos..self.filled) };
            match memchr::memchr(b'\n', buf) {
                Some(mut n) => {
                    // we want to include the newline
                    n += 1;
                    self.min_value.extend_from_slice(&buf[..n]);
                    self.pos += n;
                    break true;
                }
                None => {
                    self.min_value.extend_from_slice(buf);
                    self.pos += buf.len();
                    continue;
                }
            }
        };

        let done = self.min_value.is_empty();
        if !done && !found {
            // last line is missing newline: normalize so that higher layers can always
            // rely on the fact that there will be a newline at the end
            self.min_value.push(b'\n');
        }

        self.parsed_min_value = parse_num_with_newline(&self.min_value);
        !done
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

impl Ord for Box<SortedFile> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // ascii_number_cmp(&self.min_value, &other.min_value).reverse()
        self.parsed_min_value.cmp(&other.parsed_min_value).reverse()
    }
}

impl PartialOrd for Box<SortedFile> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Box<SortedFile> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl Eq for Box<SortedFile> {}

fn parse_num_with_newline(digits: &[u8]) -> Option<u64> {
    // ignore empty and just newline char
    if digits.len() < 2 {
        return None;
    }

    let mut res: u64 = 0;
    for &c in &digits[..digits.len() - 1] {
        res *= 10;
        let digit = (c as u64) - '0' as u64;
        res += digit;
    }
    Some(res)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ascii_number_comparison() {
        use std::cmp::Ordering::*;

        struct TestFixture {
            a: String,
            b: String,
            expected: std::cmp::Ordering,
        }

        let tests = vec![
            TestFixture {
                a: "37".to_owned(),
                b: "277".to_owned(),
                expected: Less,
            },
            TestFixture {
                a: "277".to_owned(),
                b: "277".to_owned(),
                expected: Equal,
            },
            TestFixture {
                a: "278".to_owned(),
                b: "277".to_owned(),
                expected: Greater,
            },
        ];
        for mut t in tests {
            t.a.push('\n');
            t.b.push('\n');
            assert_eq!(
                t.expected,
                ascii_number_cmp(&t.a.as_bytes(), &t.b.as_bytes())
            );
        }
    }

    #[test]
    fn test_sorted_file() {
        const FILE: &str = "files/2m.txt";
        let mut sf = SortedFile::new(FILE);
        assert_eq!("1671670171236\n".as_bytes(), &sf.min_value);

        assert_eq!(true, sf.next_line());
        assert_eq!("1671670171236\n".as_bytes(), &sf.min_value);
    }
}

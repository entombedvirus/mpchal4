use std::{
    collections::{binary_heap::PeekMut, BinaryHeap},
    env, fs,
    io::{BufRead, BufReader, BufWriter, Cursor, Write},
};

fn main() {
    let mut input: BinaryHeap<_> = env::args()
        .skip(1)
        .map(|input_file| SortedFile::new(&input_file))
        .collect();

    if input.is_empty() {
        input.push(SortedFile::new("files/2m.txt"));
        input.push(SortedFile::new("files/4m.txt"));
    }

    let output = fs::File::create("result.txt").expect("failed to create result.txt");
    let mut output = BufWriter::new(output);
    while !input.is_empty() {
        let mut sorted_file = input.peek_mut().unwrap();
        // output
        //     .write_all(&sorted_file.min_value)
        //     .expect("failed to write line to result.txt");

        if !sorted_file.next_line() {
            PeekMut::<'_, SortedFile>::pop(sorted_file);
        }
    }
}

#[derive(Debug)]
struct SortedFile {
    min_value: Vec<u8>,
    reader: BufReader<fs::File>,
}

impl SortedFile {
    fn new(file_path: &str) -> Self {
        let f = fs::File::open(file_path).expect(&format!("failed to open: {file_path}"));
        let reader = BufReader::new(f);
        let min_value = Vec::new();

        let mut ret = Self { min_value, reader };
        ret.next_line();
        ret
    }

    pub fn next_line(&mut self) -> bool {
        self.min_value.clear();
        let n = self
            .reader
            .read_until(b'\n', &mut self.min_value)
            .expect("failed to read subsequent line");
        // normalize all lines with a trailing new line so that
        // ascii comparison works
        if n > 0 && self.min_value.last() != Some(&b'\n') {
            self.min_value.push(b'\n');
        }
        n > 0
    }
}

impl Ord for SortedFile {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        ascii_number_cmp(&self.min_value, &other.min_value).reverse()
    }
}

impl PartialOrd for SortedFile {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SortedFile {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl Eq for SortedFile {}

fn ascii_number_cmp(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    match a.len().cmp(&b.len()) {
        std::cmp::Ordering::Equal => a.cmp(b),
        res => res,
    }
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

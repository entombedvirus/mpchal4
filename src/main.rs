use std::{
    collections::{binary_heap::PeekMut, BinaryHeap},
    env, fs,
    io::{BufRead, BufReader},
    sync::mpsc,
};

use rustix::fs::MetadataExt;
mod iodirect;

fn main() {
    let mut input: BinaryHeap<_> = env::args()
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

    while !input.is_empty() {
        let mut sorted_file = input.peek_mut().unwrap();
        output
            .write_bytes(&sorted_file.min_value)
            .expect("output.write_bytes failed");
        if !sorted_file.next_line() {
            PeekMut::<'_, SortedFile>::pop(sorted_file);
        }
    }
}

#[derive(Debug)]
struct SortedFile {
    min_value: Vec<u8>,
    file_size: u64,

    cur_batch_iter: std::vec::IntoIter<Vec<u8>>,

    lines: Option<mpsc::Receiver<LineBatch>>,
    worker: Option<std::thread::JoinHandle<()>>,
}

const LINEZ_BATCH_SIZE: usize = 100_000;
type LineBatch = Vec<Vec<u8>>;

impl SortedFile {
    fn new(file_path: &str) -> Self {
        let f = fs::File::open(file_path).expect(&format!("failed to open: {file_path}"));
        let file_size = f.metadata().unwrap().size();
        rustix::fs::fadvise(&f, 0, file_size, rustix::fs::Advice::Sequential)
            .expect("fadvice failed");

        let (tx, rx) = mpsc::sync_channel(10);
        let worker = std::thread::spawn(move || {
            let mut reader = BufReader::new(f);
            let mut batch = Vec::with_capacity(LINEZ_BATCH_SIZE);

            loop {
                let mut line = vec![];
                let n = reader
                    .read_until(b'\n', &mut line)
                    .expect("worker: failed to read line");
                if n == 0 {
                    // EOF
                    break;
                }
                // normalize all lines with a trailing new line so that
                // ascii comparison works
                if line.last() != Some(&b'\n') {
                    line.push(b'\n');
                }
                batch.push(line);
                if batch.len() == batch.capacity() {
                    if let Err(_) = tx.send(std::mem::replace(
                        &mut batch,
                        Vec::with_capacity(LINEZ_BATCH_SIZE),
                    )) {
                        break;
                    }
                }
            }

            if batch.len() > 0 {
                let _ = tx.send(batch);
            }
        });

        let min_value = Vec::new();
        let mut ret = Self {
            min_value,
            file_size,
            worker: Some(worker),
            lines: Some(rx),
            cur_batch_iter: vec![].into_iter(),
        };
        ret.next_line();
        ret
    }

    pub fn next_line(&mut self) -> bool {
        loop {
            if let Some(line) = self.cur_batch_iter.next() {
                self.min_value = line;
                break true;
            } else {
                match self.lines.as_ref().unwrap().recv() {
                    Ok(batch) => {
                        self.cur_batch_iter = batch.into_iter();
                        continue;
                    }
                    Err(_) => break false,
                }
            }
        }
    }
}

impl Drop for SortedFile {
    fn drop(&mut self) {
        // signal exit to worker
        drop(self.lines.take());
        self.worker
            .take()
            .unwrap()
            .join()
            .expect("worker thread panicked");
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

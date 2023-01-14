use crate::{
    iodirect::{self, ALIGN},
    simd_decimal, LINE_WIDTH_INCL_NEWLINE,
};
use std::{
    fs,
    io::{ErrorKind, Read},
};

use rustix::fs::{MetadataExt, OpenOptionsExt};

#[derive(Debug)]
pub struct SortedFile {
    pub file_size: u64,

    parsed_lines: Vec<u64>,
    // top_value_for_cmp: Option<u128>,
    parsed_line_pos: usize,
    partial_line_bytes: usize,

    reader: fs::File,
    aligned_buf: Box<[u8]>,
    pos: usize,
    filled: usize,
}

impl SortedFile {
    pub fn new(file_path: &str) -> Self {
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
            let alloc_size = ALIGN + SZ;
            let layout = std::alloc::Layout::from_size_align(alloc_size, ALIGN).unwrap();
            let ptr = std::alloc::alloc_zeroed(layout);
            let slice = std::slice::from_raw_parts_mut(ptr, alloc_size);
            Box::from_raw(slice)
        };

        let mut ret = Self {
            file_size,

            parsed_lines: Vec::new(),
            // top_value_for_cmp: None,
            parsed_line_pos: 0,
            partial_line_bytes: 0,

            reader,
            aligned_buf,
            pos: 0,
            filled: 0,
        };
        ret.fill_parsed_lines();
        // ret.top_value_for_cmp = ret.parse_cmp_value();
        ret
    }

    // fn get_unread_slice(&self) -> &[u8] {
    //     // SAFETY: next + fill ensures that these indices are always safe
    //     unsafe {
    //         self.aligned_buf
    //             .get_unchecked(self.parsed_line_pos..self.filled)
    //     }
    // }

    #[inline]
    pub fn peek(&self) -> Option<&u64> {
        // self.top_value_for_cmp
        self.parsed_lines.get(self.parsed_line_pos)
    }

    // fn parse_cmp_value(&mut self) -> Option<u128> {
    //     self.fill_parsed_lines();
    //     let ascii_num_wo_nl = self.get_unread_slice().get(..LINE_WIDTH_INCL_NEWLINE - 1)?;
    //     let mut bytes = [0; 16];
    //     bytes[3..].copy_from_slice(ascii_num_wo_nl);
    //     bytes.reverse();
    //     Some(u128::from_le_bytes(bytes))
    // }

    #[inline]
    pub fn next(&mut self) {
        let start = self.parsed_line_pos;
        if start < self.parsed_lines.len() {
            self.parsed_line_pos += 1;
            self.fill_parsed_lines();
            // self.top_value_for_cmp = self.parse_cmp_value();
        }
    }

    #[inline]
    pub fn peek_bytes(&self) -> Option<&[u8; LINE_WIDTH_INCL_NEWLINE]> {
        let start = self.pos + self.parsed_line_pos * LINE_WIDTH_INCL_NEWLINE;
        if start + LINE_WIDTH_INCL_NEWLINE > self.filled {
            return None;
        }
        let bytes = unsafe {
            self.aligned_buf
                .get_unchecked(start..start + LINE_WIDTH_INCL_NEWLINE)
        };
        Some(bytes.try_into().unwrap())
    }

    fn fill_parsed_lines(&mut self) {
        if self.parsed_line_pos < self.parsed_lines.len() {
            return;
        }

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

        let num_complete_lines = buf.len() / LINE_WIDTH_INCL_NEWLINE;
        self.partial_line_bytes = buf.len() % LINE_WIDTH_INCL_NEWLINE;
        simd_decimal::parse_packed_4bit::<6, LINE_WIDTH_INCL_NEWLINE>(
            &buf[..num_complete_lines * LINE_WIDTH_INCL_NEWLINE],
            &mut self.parsed_lines,
        );
        // simd_decimal::parse_incomplete::<1, LINE_WIDTH_INCL_NEWLINE>(
        //     &buf[..num_complete_lines * LINE_WIDTH_INCL_NEWLINE],
        //     &mut self.parsed_lines,
        // );
        // simd_decimal::parse_decimals::<4, LINE_WIDTH_INCL_NEWLINE>(
        //     &buf[..num_complete_lines * LINE_WIDTH_INCL_NEWLINE],
        //     &mut self.parsed_lines,
        // );

        let n = self.partial_line_bytes;
        // save the partial line at beginning so that we can copy
        // it to the right place next time
        self.aligned_buf
            .copy_within(self.filled - n..self.filled, 0);
        self.filled -= n;
        assert!((self.filled - self.pos) % LINE_WIDTH_INCL_NEWLINE == 0);

        self.parsed_line_pos = 0;
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

impl PartialOrd for SortedFile {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // we want a min heap and stdlib does max heap
        Some(self.cmp(other).reverse())
    }
}

impl Ord for SortedFile {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.peek().cmp(&other.peek())
    }
}

impl PartialEq for SortedFile {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.peek() == other.peek()
    }
}

impl Eq for SortedFile {}

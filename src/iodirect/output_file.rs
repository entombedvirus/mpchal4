use crate::iodirect::ALIGN;
use crate::iodirect::CHUNK_SIZE;
use crate::iodirect::LINE_WIDTH_INCL_NEWLINE;
use std::path::Path;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::TryRecvError;
use std::{
    fs,
    io::{self, Cursor, Write},
    sync::mpsc,
};

use std::os::unix::fs::{FileExt, OpenOptionsExt};

type Buf = Cursor<Box<[u8]>>;

pub struct OutputFile {
    cur_buf: Buf,
    io_chan: Option<mpsc::Sender<Buf>>,
    worker: Option<std::thread::JoinHandle<()>>,
    buf_pool: mpsc::Receiver<Buf>,

    fmt: TimeFormatter<LINE_WIDTH_INCL_NEWLINE, 4>,
}

impl OutputFile {
    pub fn new<P: AsRef<Path>>(path: P, expected_file_size: usize) -> OutputFile {
        let path = path.as_ref().to_owned();
        let (send, recv) = mpsc::channel();
        let io_chan: Option<mpsc::Sender<Buf>> = Some(send.clone());

        let (buf_pool_send, buf_pool_recv) = mpsc::channel();

        let worker = std::thread::spawn(move || {
            let inner = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                // .custom_flags(libc::O_DIRECT)
                .open(path)
                .expect("failed to create result.txt");
            rustix::fs::fallocate(
                &inner,
                rustix::fs::FallocateFlags::KEEP_SIZE,
                0,
                expected_file_size as u64,
            )
            .expect("fallocate failed");

            const Q_DEPTH: usize = 256;

            struct Payload {
                offset: usize,
                bytes: Box<[u8]>,
            }
            struct Batch {
                q: Vec<Payload>,
                off: usize,
                padn: usize,

                ring: rio::Rio,
                file: fs::File,

                buf_pool_recycle: mpsc::Sender<Buf>,
            }
            impl Batch {
                fn new(file: fs::File, buf_recycle: mpsc::Sender<Buf>) -> Self {
                    Self {
                        q: Vec::with_capacity(Q_DEPTH),
                        off: 0,
                        padn: 0,
                        ring: rio::new().expect("failed to create io_uring"),
                        file,
                        buf_pool_recycle: buf_recycle,
                    }
                }

                fn push(&mut self, buf: Buf) {
                    let batch = &mut self.q;
                    let padn = self.padn;
                    let mut buf_len = buf.position() as usize;
                    let mut buf = buf.into_inner();
                    if buf_len % ALIGN != 0 {
                        // this happens on the very last write
                        assert_eq!(
                            padn, 0,
                            "non-aligned write is only expected once at the very end"
                        );
                        self.padn = ALIGN - buf_len % ALIGN;
                        assert!(
                            buf_len + padn <= buf.len(),
                            "buf will resize, which will destroy alignment guarantees"
                        );
                        buf[buf_len..buf_len + padn].fill(0_u8);
                        buf_len += padn;
                    }
                    batch.push(Payload {
                        offset: self.off,
                        bytes: buf,
                    });
                    self.off += buf_len;
                }

                fn flush(&mut self) {
                    if self.q.len() == 0 {
                        return;
                    }
                    let completions = self.q.iter().map(|payload| {
                        self.ring
                            .write_at(&self.file, &payload.bytes, payload.offset as u64)
                    });
                    for c in completions {
                        c.wait().expect("output file: worker: io failure");
                        // TODO: assert not short write
                    }
                    for payload in self.q.drain(..) {
                        if let Err(err) = self.buf_pool_recycle.send(Cursor::new(payload.bytes)) {
                            eprintln!("failed to send buf back in pool: {err}");
                        }
                    }
                }

                fn clear(&mut self) {
                    self.q.clear();
                }

                fn len(&self) -> usize {
                    self.q.len()
                }

                fn capacity(&self) -> usize {
                    self.q.capacity()
                }
            }

            let mut batch = Batch::new(inner, buf_pool_send);
            loop {
                batch.clear();
                let res = recv.recv();
                match res {
                    Ok(buf) => {
                        batch.push(buf);
                        // optimistically try to get N more things
                        while batch.len() < batch.capacity() {
                            if let Ok(buf) = recv.try_recv() {
                                batch.push(buf);
                            } else {
                                break;
                            }
                        }
                        batch.flush();
                        continue;
                    }
                    Err(RecvError) => break,
                }
            }

            // truncate file to expected size since we might've
            // written padding zero bytes for O_DIRECT alignment
            rustix::fs::ftruncate(&batch.file, (batch.off - batch.padn) as u64)
                .expect("ftruncate failed");
        });

        Self {
            io_chan,
            cur_buf: Self::new_buf(),
            worker: Some(worker),
            buf_pool: buf_pool_recv,
            fmt: TimeFormatter::new(),
        }
    }

    #[allow(dead_code)]
    #[inline]
    pub fn write_u64(&mut self, v: u64) -> io::Result<()> {
        self.fmt.serialized_bytes(v);
        let line = &self.fmt.last_serialized;
        Self::do_write_bytes(
            line,
            &mut self.cur_buf,
            &self.buf_pool,
            &self.io_chan.as_ref().unwrap(),
        )
    }

    #[inline]
    pub fn write_bytes(&mut self, line: &[u8; LINE_WIDTH_INCL_NEWLINE]) -> io::Result<()> {
        Self::do_write_bytes(
            line,
            &mut self.cur_buf,
            &self.buf_pool,
            &self.io_chan.as_ref().unwrap(),
        )
    }

    fn do_write_bytes(
        line: &[u8; LINE_WIDTH_INCL_NEWLINE],
        cur_buf: &mut Cursor<Box<[u8]>>,
        buf_pool: &mpsc::Receiver<Buf>,
        io_chan: &mpsc::Sender<Buf>,
    ) -> io::Result<()> {
        let buf = cur_buf.get_ref();
        let cap = buf.len() - cur_buf.position() as usize;
        if cap < line.len() {
            let (partial, rem) = line.split_at(cap);
            let wr = cur_buf.write(partial).unwrap();
            assert_eq!(wr, partial.len(), "write_bytes: partial: short write");
            Self::flush(cur_buf, &buf_pool, io_chan).expect("write_bytes: flush failed");
            let wr = cur_buf.write(rem).unwrap();
            assert_eq!(wr, rem.len(), "write_bytes: partial: short write");
            return Ok(());
        }

        cur_buf.write(line).map(|_| ())
    }

    fn flush(
        cur_buf: &mut Cursor<Box<[u8]>>,
        buf_pool: &mpsc::Receiver<Buf>,
        io_chan: &mpsc::Sender<Buf>,
    ) -> io::Result<()> {
        if cur_buf.position() == 0 {
            return Ok(());
        }

        let new_buf_to_use = match buf_pool.try_recv() {
            Ok(buf) => buf,
            Err(_) => Self::new_buf(),
        };
        let cur = std::mem::replace(cur_buf, new_buf_to_use);
        io_chan.send(cur).unwrap();
        Ok(())
    }

    fn new_buf() -> Buf {
        let layout = std::alloc::Layout::from_size_align(CHUNK_SIZE, ALIGN).unwrap();
        let boxed_slice = unsafe {
            let ptr = std::alloc::alloc_zeroed(layout);
            let slice = std::slice::from_raw_parts_mut(ptr, CHUNK_SIZE);
            Box::from_raw(slice)
        };
        Cursor::new(boxed_slice)
    }
}

impl Drop for OutputFile {
    fn drop(&mut self) {
        // write buffered lines, if any
        Self::flush(
            &mut self.cur_buf,
            &self.buf_pool,
            self.io_chan.as_ref().unwrap(),
        )
        .expect("write_bytes: flush failed");

        // signal worker thread to exit by dropping the sender
        let sender = self.io_chan.take();
        drop(sender);

        // wait for worker to exit
        self.worker
            .take()
            .unwrap()
            .join()
            .expect("drop: worker panicked");
    }
}

#[allow(dead_code)]
struct TimeFormatter<const LINE_WIDTH: usize, const N: usize> {
    last_prefix: u64,
    last_serialized: [u8; LINE_WIDTH],
}

// 2 digit decimal look up table
#[allow(dead_code)]
static DEC_DIGITS_LUT: &[u8; 200] = b"0001020304050607080910111213141516171819\
      2021222324252627282930313233343536373839\
      4041424344454647484950515253545556575859\
      6061626364656667686970717273747576777879\
      8081828384858687888990919293949596979899";

impl<const LINE_WIDTH: usize> TimeFormatter<LINE_WIDTH, 4> {
    fn new() -> Self {
        Self {
            last_prefix: 0,
            last_serialized: "0123456789abc\n".as_bytes().try_into().unwrap(),
        }
    }
    #[allow(dead_code)]
    fn serialized_bytes(&mut self, v: u64) {
        let d = 10_000_u64;
        let prefix = v / d;
        if prefix == self.last_prefix {
            let lut_ptr = DEC_DIGITS_LUT.as_ptr();
            let buf_ptr = self.last_serialized.as_mut_ptr();
            // likely that the only the last N digits are different in case of sorted numbers
            let suffix = (v % d) as usize;
            // turn the first two and last two digits to lookup table index
            let d1 = (suffix / 100) << 1;
            let d2 = (suffix % 100) << 1;
            unsafe {
                core::ptr::copy_nonoverlapping(lut_ptr.add(d1), buf_ptr.add(LINE_WIDTH - 4 - 1), 2);
                core::ptr::copy_nonoverlapping(lut_ptr.add(d2), buf_ptr.add(LINE_WIDTH - 2 - 1), 2);
            }
        } else {
            let num_digits = (LINE_WIDTH - 1) as u32;
            let mut rem = v;
            for i in 0..num_digits {
                let divisor = 10_u64.pow(num_digits - i - 1);
                let d = rem / divisor;
                self.last_serialized[i as usize] = b'0' + d as u8;
                rem = rem % divisor;
            }
            self.last_prefix = prefix;
            write!(&mut self.last_serialized[..LINE_WIDTH - 1], "{v}").unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_formatter() {
        let mut fmt = TimeFormatter::<14, 4>::new();
        let expected: [u8; 14] = "1671669405500\n".as_bytes().try_into().unwrap();
        fmt.serialized_bytes(1671669405500_u64);
        assert_eq!(fmt.last_serialized, expected);

        let expected: [u8; 14] = "1671669405596\n".as_bytes().try_into().unwrap();
        fmt.serialized_bytes(1671669405596_u64);
        assert_eq!(fmt.last_serialized, expected);

        let expected: [u8; 14] = "2671669401116\n".as_bytes().try_into().unwrap();
        fmt.serialized_bytes(2671669401116_u64);
        assert_eq!(fmt.last_serialized, expected);

        let expected: [u8; 14] = "2671669400006\n".as_bytes().try_into().unwrap();
        fmt.serialized_bytes(2671669400006_u64);
        assert_eq!(fmt.last_serialized, expected);
    }
}

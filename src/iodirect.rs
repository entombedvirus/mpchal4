use std::{
    fs,
    io::{self, Cursor, Write},
    sync::mpsc,
};

use rustix::fs::{FileExt, OpenOptionsExt};

const CHUNK_SIZE: usize = 1 << 20;
pub const ALIGN: usize = 4096;

type Buf = Cursor<Box<[u8]>>;

fn new_buf() -> Buf {
    let layout = std::alloc::Layout::from_size_align(CHUNK_SIZE, ALIGN).unwrap();
    let boxed_slice = unsafe {
        let ptr = std::alloc::alloc_zeroed(layout);
        let slice = std::slice::from_raw_parts_mut(ptr, CHUNK_SIZE);
        Box::from_raw(slice)
    };
    Cursor::new(boxed_slice)
}

pub struct File {
    cur_buf: Buf,
    io_chan: Option<mpsc::Sender<Buf>>,
    worker: Option<std::thread::JoinHandle<()>>,
    buf_pool: mpsc::Receiver<Buf>,

    fmt: TimeFormatter<14, 4>,
}

impl File {
    pub fn new(path: &str, expected_file_size: usize) -> File {
        let inner = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
            .expect("failed to create result.txt");
        rustix::fs::fallocate(
            &inner,
            rustix::fs::FallocateFlags::KEEP_SIZE,
            0,
            expected_file_size as u64,
        )
        .expect("fallocate failed");

        let (send, recv) = mpsc::channel();
        let io_chan: Option<mpsc::Sender<Buf>> = Some(send.clone());

        let (buf_pool_send, buf_pool_recv) = mpsc::channel();

        let worker = std::thread::spawn(move || {
            let mut off = 0_usize;
            let mut padn = 0_usize;
            for buf in recv {
                let mut buf_len = buf.position() as usize;
                let mut buf = buf.into_inner();
                if buf_len % ALIGN != 0 {
                    // this happens on the very last write
                    assert_eq!(
                        padn, 0,
                        "non-aligned write is only expected once at the very end"
                    );
                    padn = ALIGN - buf_len % ALIGN;
                    assert!(
                        buf_len + padn <= buf.len(),
                        "buf will resize, which will destroy alignment guarantees"
                    );
                    buf[buf_len..buf_len + padn].fill(0_u8);
                    buf_len += padn;
                }
                inner
                    .write_all_at(&buf[..buf_len], off as u64)
                    .expect("worker: write_all_at failed");
                off += buf_len;

                if let Err(err) = buf_pool_send.send(Cursor::new(buf)) {
                    eprintln!("failed to send buf back in pool: {err}");
                }
            }

            // truncate file to expected size since we might've
            // written padding zero bytes for O_DIRECT alignment
            rustix::fs::ftruncate(&inner, (off - padn) as u64).expect("ftruncate failed");
        });

        Self {
            io_chan,
            cur_buf: new_buf(),
            worker: Some(worker),
            buf_pool: buf_pool_recv,
            fmt: TimeFormatter::new(),
        }
    }

    #[inline]
    pub fn write_u64(&mut self, v: u64) -> io::Result<()> {
        let line = self.fmt.serialized_bytes(v);

        let buf = self.cur_buf.get_ref();
        let cap = buf.len() - self.cur_buf.position() as usize;
        if cap < line.len() {
            let (partial, rem) = line.split_at(cap);
            let wr = self.cur_buf.write(partial).unwrap();
            assert_eq!(wr, partial.len(), "write_bytes: partial: short write");
            self.flush().expect("write_bytes: flush failed");
            let wr = self.cur_buf.write(rem).unwrap();
            assert_eq!(wr, rem.len(), "write_bytes: partial: short write");
            return Ok(());
        }

        self.cur_buf.write(&line).map(|_| ())
    }

    #[inline]
    pub fn write_bytes(&mut self, mut line: &[u8]) -> io::Result<()> {
        let buf = self.cur_buf.get_ref();
        let cap = buf.len() - self.cur_buf.position() as usize;
        if cap < line.len() {
            let (partial, rem) = line.split_at(cap);
            line = rem;
            assert_eq!(
                self.cur_buf.write(partial).unwrap(),
                partial.len(),
                "write_bytes: partial: short write"
            );
            self.flush().expect("write_bytes: flush failed");
        }
        assert_eq!(
            self.cur_buf.write(line).unwrap(),
            line.len(),
            "write_bytes: short write"
        );
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.cur_buf.position() == 0 {
            return Ok(());
        }

        let new_buf_to_use = match self.buf_pool.try_recv() {
            Ok(buf) => buf,
            Err(_) => new_buf(),
        };
        let cur = std::mem::replace(&mut self.cur_buf, new_buf_to_use);
        self.io_chan.as_ref().unwrap().send(cur).unwrap();
        Ok(())
    }
}

impl Drop for File {
    fn drop(&mut self) {
        // write buffered lines, if any
        self.flush().expect("drop: flush failed");

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

struct TimeFormatter<const LINE_WIDTH: usize, const N: usize> {
    last_prefix: u64,
    last_serialized: [u8; LINE_WIDTH],
}

impl<const LINE_WIDTH: usize, const N: usize> TimeFormatter<LINE_WIDTH, N> {
    fn new() -> Self {
        Self {
            last_prefix: 0,
            last_serialized: "0123456789abc\n".as_bytes().try_into().unwrap(),
        }
    }
    fn serialized_bytes(&mut self, v: u64) -> [u8; LINE_WIDTH] {
        let d = 10u64.pow(N as u32);
        let prefix = v / d;
        if prefix == self.last_prefix {
            let suffix = v % d;
            // likely in case of sorted numbers
            write!(
                &mut self.last_serialized[LINE_WIDTH - N - 1..LINE_WIDTH - 1],
                "{suffix:0width$}",
                width = N,
            )
            .unwrap();
        } else {
            self.last_prefix = prefix;
            write!(&mut self.last_serialized[..LINE_WIDTH - 1], "{v}").unwrap();
        }
        self.last_serialized.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_formatter() {
        let mut fmt = TimeFormatter::<14, 4>::new();
        let expected: [u8; 14] = "1671669405500\n".as_bytes().try_into().unwrap();
        assert_eq!(fmt.serialized_bytes(1671669405500_u64), expected);

        let expected: [u8; 14] = "1671669405596\n".as_bytes().try_into().unwrap();
        assert_eq!(fmt.serialized_bytes(1671669405596_u64), expected);

        let expected: [u8; 14] = "2671669401116\n".as_bytes().try_into().unwrap();
        assert_eq!(fmt.serialized_bytes(2671669401116_u64), expected);

        let expected: [u8; 14] = "2671669400006\n".as_bytes().try_into().unwrap();
        assert_eq!(fmt.serialized_bytes(2671669400006_u64), expected);
    }
}

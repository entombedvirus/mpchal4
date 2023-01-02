use std::{
    fs,
    io::{self, Cursor, Write},
    sync::mpsc,
};

const CHUNK_SIZE: usize = 1 << 20;

type Buf = Cursor<Vec<u8>>;

fn new_buf() -> Buf {
    let layout = std::alloc::Layout::from_size_align(CHUNK_SIZE, 4096).unwrap();
    let vec = unsafe {
        let ptr = std::alloc::alloc_zeroed(layout);
        std::vec::Vec::from_raw_parts(ptr, 0, CHUNK_SIZE)
    };
    Cursor::new(vec)
}

pub struct File {
    buf: Buf,
    io_chan: Option<mpsc::Sender<Buf>>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl File {
    pub fn new(path: &str, expected_file_size: usize) -> File {
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

        let (send, recv) = mpsc::channel();
        let io_chan: Option<mpsc::Sender<Buf>> = Some(send.clone());

        let worker = std::thread::spawn(move || {
            let ring = rio::new().expect("io_uring failed");
            let mut off = 0_usize;
            for buf in recv {
                let buf_len = buf.position() as usize;
                let buf = buf.into_inner();
                let wr = ring
                    .write_at(&inner, &&buf[..buf_len], off as u64)
                    .wait()
                    .expect("write_at failed");
                assert_eq!(wr, buf_len, "short write: wrote {wr} / {buf_len}");
                off += buf_len;
            }
        });

        Self {
            io_chan,
            buf: new_buf(),
            worker: Some(worker),
        }
    }

    pub fn write_bytes(&mut self, line: &[u8]) -> io::Result<()> {
        let buf = self.buf.get_ref();
        let cap = buf.capacity() - self.buf.position() as usize;
        if cap < line.len() {
            self.flush().expect("write_bytes: flush failed");
        }
        assert_eq!(
            self.buf.write(line).unwrap(),
            line.len(),
            "write_bytes: short write"
        );
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.buf.position() == 0 {
            return Ok(());
        }

        let cur = std::mem::replace(&mut self.buf, new_buf());
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

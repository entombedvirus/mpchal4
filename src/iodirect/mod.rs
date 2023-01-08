pub(crate) mod output_file;
pub(crate) mod sorted_file;

const CHUNK_SIZE: usize = 1 << 20;
pub const ALIGN: usize = 4096;
pub const LINE_WIDTH_INCL_NEWLINE: usize = 14;

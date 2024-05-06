use std::cmp::min;

/// Iterates the file in `n` chunks, but with respect to line endings.
/// This helps us to distribute the workload exactly between multiple
/// threads.
pub struct ChunkIter<'a> {
    bytes_per_chunk: usize,
    file_bytes: &'a [u8],
    /// This variable is mutated and keeps track of the progress.
    consumed_bytes: usize,
}

impl<'a> ChunkIter<'a> {
    pub fn new(file_bytes: &'a [u8], chunk_count: usize) -> Self {
        let bytes_per_chunk = file_bytes.len().div_ceil(chunk_count);
        Self {
            file_bytes,
            bytes_per_chunk,
            consumed_bytes: 0,
        }
    }
}

impl<'a> Iterator for ChunkIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let bytes_left = self.file_bytes.len() - self.consumed_bytes;
        if bytes_left == 0 {
            return None;
        }

        let i_begin = self.consumed_bytes;
        // -1: because the given byte might already be a newline
        let i_end_min = i_begin + min(self.bytes_per_chunk, bytes_left) - 1;

        let search_slice = &self.file_bytes[i_end_min..];
        let i_end_actual = memchr::memchr(b'\n', search_slice).unwrap() + i_end_min;

        // include final newline here
        let chunk = &self.file_bytes[i_begin..i_end_actual + 1];

        self.consumed_bytes += chunk.len();

        Some(chunk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_iter_bytes_per_chunk() {
        let data = "a".repeat(15);
        let iter = ChunkIter::new(data.as_bytes(), 2);

        assert_eq!(iter.bytes_per_chunk, 8, "must be rounded up");
    }

    #[test]
    fn test_chunk_iter_chunk_size_aligns_with_newlines() {
        let data = "aaa\nbbb\nccc\nddd\neee\n";
        assert_eq!(data.len(), 20);
        let iter = ChunkIter::new(data.as_bytes(), 5);
        assert_eq!(iter.bytes_per_chunk, 4);

        let mut iter = iter.map(|data| core::str::from_utf8(data).unwrap());

        assert_eq!(Some("aaa\n"), iter.next());
        assert_eq!(Some("bbb\n"), iter.next());
        assert_eq!(Some("ccc\n"), iter.next());
        assert_eq!(Some("ddd\n"), iter.next());
        assert_eq!(Some("eee\n"), iter.next());
    }

    #[test]
    fn test_chunk_iter_evenly_splittable() {
        let data = "aaa\nbbbb\nccccc\ndddddd\neeeeeee\n";
        assert_eq!(data.len(), 30);
        let iter = ChunkIter::new(data.as_bytes(), 3);
        assert_eq!(iter.bytes_per_chunk, 10, "must be rounded up");

        let mut iter = iter.map(|data| core::str::from_utf8(data).unwrap());

        assert_eq!(Some("aaa\nbbbb\nccccc\n"), iter.next());
        assert_eq!(Some("dddddd\neeeeeee\n"), iter.next());
    }

    #[test]
    fn test_chunk_iter_not_evenly_splittable() {
        let data = "aa\nbb\ncc\ndd\nee\nff\ngg\n".repeat(2);
        assert_eq!(data.len(), 42);
        let iter = ChunkIter::new(data.as_bytes(), 4);
        assert_eq!(iter.bytes_per_chunk, 11, "must be rounded up");

        let mut iter = iter.map(|data| core::str::from_utf8(data).unwrap());

        assert_eq!(Some("aa\nbb\ncc\ndd\n"), iter.next());
        assert_eq!(Some("ee\nff\ngg\naa\n"), iter.next());
        assert_eq!(Some("bb\ncc\ndd\nee\n"), iter.next());
        assert_eq!(Some("ff\ngg\n"), iter.next());
    }
}

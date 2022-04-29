// lib.rs
// mbox-indexer
// Copyright 2022 Andrew Morrow. All rights reserved.

use memchr::{memchr, memmem};
use std::io::{self, BufRead, Read, Seek};

pub struct MboxReader<R> {
    inner: MagicReader<R>,
}

pub struct MboxEntry<'a, R> {
    inner: &'a mut MagicReader<R>,
}

impl<'a, R: Read> MboxReader<R> {
    pub fn new(inner: R) -> Self {
        MboxReader {
            inner: MagicReader::new(inner),
        }
    }

    // Cannot implement std::iter::Iterator because of self-referential struct
    pub fn next(&'a mut self) -> io::Result<Option<MboxEntry<'a, R>>> {
        if self.inner.eof()? {
            return Ok(None);
        }
        if !self.inner.eom() {
            self.inner.skip_message()?;
            if self.inner.eof()? {
                return Ok(None);
            }
        }
        assert!(self.inner.eom());
        self.inner.reset_eom();
        Ok(Some(MboxEntry {
            inner: &mut self.inner,
        }))
    }
}

impl<'a, R: Read> Read for MboxEntry<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

const DEFAULT_CAPACITY: usize = 8192;
const MAGIC_WORD: [u8; 6] = [0x0A, 0x46, 0x72, 0x6F, 0x6D, 0x20];

/// MagicReader reads bytes until it reaches the "magic word", `From `. When it reaches the "magic
/// word", it will stop reading (i.e. return 0 bytes). The `eom` function will return true.
/// By calling `reset_eom`, you can read the next message.
struct MagicReader<R> {
    inner: R,
    buffer: Vec<u8>,
    buffer_end: usize,
    ready_start: usize,
    ready_end: usize,
    held_back: usize,
    next_message_start: Option<usize>,
}

impl<R: Read> MagicReader<R> {
    fn new(inner: R) -> Self {
        MagicReader {
            inner,
            buffer: vec![0; DEFAULT_CAPACITY],
            buffer_end: 0,
            ready_start: 0,
            ready_end: 0,
            held_back: 0,
            next_message_start: None,
        }
    }

    /// Returns true if the reader has stopped before the beginning of the next message. Returns
    /// false in all other cases, including end of file.
    ///
    /// Note: You may have to read until 0 bytes are returned before this function returns true.
    fn eom(&self) -> bool {
        self.next_message_start
            .map_or(false, |i| i == self.ready_start)
    }

    /// Returns true if there is no more data to be read. Returns `io::Result` because it may have
    /// to read from the underlying stream.
    fn eof(&mut self) -> io::Result<bool> {
        Ok(!self.eom() && self.fill_buf()?.is_empty())
    }

    /// If called when `self.eom() == false`, this triggers undefined behavior
    fn reset_eom(&mut self) {
        assert!(self.eom());
        self.next_message_start = None;
    }

    /// Skips all remaining bytes in the current message. The reader will return 0 bytes until
    /// after calling `reset_eom()`.
    fn skip_message(&mut self) -> io::Result<()> {
        assert!(self.eom());
        todo!()
    }
}

impl<R: Read> Read for MagicReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let available = self.fill_buf()?;
        let copied = available.len().min(buf.len());
        (&mut buf[..copied]).copy_from_slice(&available[..copied]);
        self.consume(copied);
        Ok(copied)
    }
}

impl<R: Read> BufRead for MagicReader<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.ready_start != self.ready_end {
            // don't refill until all bytes have been consumed
            return Ok(&self.buffer[self.ready_start..self.ready_end]);
        }

        if self.next_message_start.is_some() {
            assert!(self.eom());
            return Ok(&[]);
        }

        if self.ready_end == self.held_back {
            // we read everything in the buffer and it's time to restart at the beginning, possibly
            // copying held back bytes
            let num_held_back = self.buffer_end - self.held_back;
            if num_held_back > 0 {
                self.buffer.copy_within(self.held_back..self.buffer_end, 0);
            }

            self.ready_start = 0;
            self.ready_end = 0; // the bytes aren't ready until we've checked them for the magic word
            self.buffer_end = num_held_back;
            self.held_back = num_held_back; // because it's equal to buffer_end, 0 bytes are held back

            while self.buffer_end < self.buffer.len() {
                let bytes_read = self.inner.read(&mut self.buffer[self.buffer_end..])?;
                self.buffer_end += bytes_read;
                self.held_back = self.buffer_end;
                if bytes_read == 0 {
                    break;
                }
            }

            if self.buffer_end >= MAGIC_WORD.len() {
                // as long as there are six bytes or more in the buffer, we want to check for
                // newlines in the last five and hold those back
                // if there are five or fewer bytes, then we're at source EOF and don't need to
                let last5 = &self.buffer[self.buffer_end - 5..self.buffer_end];
                if let Some(newline_idx) = memchr(b'\n', last5) {
                    self.held_back = self.buffer_end - 5 + newline_idx;
                }
            }
        } else {
            // we called reset_eom() and are continuing to read pre-buffered content
            // we don't want to reset any offsets - they are all still accurate
        }

        // assumptions:
        // this has to be true, because it's already been checked
        assert_eq!(self.ready_start, self.ready_end);

        if let Some(newline_idx) =
            memmem::find(&self.buffer[self.ready_start..self.held_back], &MAGIC_WORD)
        {
            self.ready_end = newline_idx + 1;
            self.next_message_start = Some(newline_idx + 1);
        } else {
            self.ready_end = self.held_back;
        }

        Ok(&self.buffer[self.ready_start..self.ready_end])
    }

    fn consume(&mut self, amt: usize) {
        assert!(amt <= (self.ready_end - self.ready_start));
        self.ready_start += amt;
    }
}

impl<R: Seek> MagicReader<R> {
    fn stream_position(&mut self) -> io::Result<u64> {
        // the bytes from ready_start to buffer_end have been read from the inner reader, but have
        // not been read by this reader
        self.inner
            .stream_position()
            .map(|i| i - (self.buffer_end - self.ready_start) as u64)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn one_byte_reads() {
        // read one byte at a time and check it behaves properly
        let input = b"From line1\nFrom line2";
        let mut reader = MagicReader::new(Cursor::new(input));
        let mut buf: [u8; 1] = [0];
        let mut full: Vec<u8> = Vec::with_capacity(input.len());
        for _ in 0..11 {
            assert_eq!(1, reader.read(&mut buf).unwrap());
            full.push(buf[0]);
        }
        assert_eq!(0, reader.read(&mut buf).unwrap());
        assert_eq!(&full, &input[..11]);
        assert!(reader.eom());
        reader.reset_eom();
        for _ in 0..10 {
            assert_eq!(1, reader.read(&mut buf).unwrap());
            full.push(buf[0]);
        }
        assert_eq!(0, reader.read(&mut buf).unwrap());
        assert_eq!(&full, &input);
        assert!(reader.eof().unwrap());
        assert!(!reader.eom());
    }

    #[test]
    fn crlf() {
        // use CRLF line endings and check it works right
        // TODO
    }

    #[test]
    fn exact_read_sizes() {
        // read the exact number of bytes in the first line, then make sure it returns 0 bytes, then the next line
        let input = b"From line1\nFrom line2";
        let mut reader = MagicReader::new(Cursor::new(input));
        let mut buf: [u8; 11] = [0; 11];
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 11);
        assert!(reader.eom());
        assert_eq!(buf[..], input[..11]);
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 0);
        reader.reset_eom();
        assert!(!reader.eom());
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 10);
        assert_eq!(buf[..bytes_read], input[11..]);
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 0);
        assert!(reader.eof().unwrap());
        assert!(!reader.eom());
    }

    #[test]
    fn page_boundary() {
        // let's try a bunch of read sizes with the magic word just off the page boundary
        for offset in 0..MAGIC_WORD.len() + 1 {
            let mut input: Vec<u8> = vec![0; DEFAULT_CAPACITY];
            for _ in 0..offset {
                input.pop();
            }
            input.extend_from_slice(&MAGIC_WORD);
            input.extend_from_slice(b"test123");
            let input = input;

            for block_size in 0..32 {
                let mut buffer: Vec<u8> = vec![0; 128 + block_size];
                let mut result: Vec<u8> = Vec::with_capacity(DEFAULT_CAPACITY + 16);
                let mut reader = MagicReader::new(Cursor::new(&input));
                while !reader.eom() {
                    let read = reader.read(&mut buffer).unwrap();
                    assert!(read > 0);
                    // if read == 0 {
                    //     let eom = reader.eom();
                    //     let eof = reader.eof().unwrap();
                    //     println!("{}, {}", eom, eof);
                    // }
                    result.extend_from_slice(&buffer[..read]);
                }
                // 1 byte for the newline, plus the total number of null bytes
                assert_eq!(result.len(), 1 + DEFAULT_CAPACITY - offset);
                assert_eq!(*result.last().unwrap(), b'\n');
                let read = reader.read(&mut buffer).unwrap();
                assert_eq!(read, 0);
                reader.reset_eom();

                let mut v: Vec<u8> = Vec::new();
                reader.read_to_end(&mut v).unwrap();
                assert!(reader.eof().unwrap());
                assert_eq!(b"From test123", &v[..]);
            }
        }
    }

    #[test]
    fn mbox_reader() {
        // TODO: create an MboxReader and call next() until it stops
    }
}

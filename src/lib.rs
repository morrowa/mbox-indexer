// lib.rs
// mbox-indexer
// Copyright 2022 Andrew Morrow. All rights reserved.

use memchr::{memchr, memmem};
use std::io::{self, BufRead, Read};

pub struct MboxReader<R> {
    inner: MessageBoundaryReader<R>,
    first: bool,
}

pub struct MboxEntry<'a, R> {
    inner: &'a mut MessageBoundaryReader<R>,
}

impl<'a, R: Read> MboxReader<R> {
    pub fn new(inner: R) -> Self {
        MboxReader {
            inner: MessageBoundaryReader::new(inner),
            first: true,
        }
    }

    // Cannot implement std::iter::Iterator because of self-referential struct
    pub fn next(&'a mut self) -> io::Result<Option<MboxEntry<'a, R>>> {
        if self.inner.eof()? {
            return Ok(None);
        }
        if self.first {
            self.first = false;
            return Ok(Some(MboxEntry {
                inner: &mut self.inner,
            }));
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

impl<'a, R: Read> BufRead for MboxEntry<'a, R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.inner.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.inner.consume(amt)
    }
}

const DEFAULT_CAPACITY: usize = 8192;
const MAGIC_WORD: [u8; 6] = [0x0A, 0x46, 0x72, 0x6F, 0x6D, 0x20];

/// MessageBoundaryReader reads bytes until it reaches the "magic word": `From` preceded by a
/// newline (0x0A) and followed by a space (0x20). When it reaches the "magic word", it will stop
/// reading (i.e. return 0 bytes) and the `eom` function will return true. To read the next message,
/// call `reset_eom`.
struct MessageBoundaryReader<R> {
    inner: R,
    buffer: Vec<u8>,
    buffer_end: usize,
    ready_start: usize,
    ready_end: usize,
    held_back: usize,
    next_message_start: Option<usize>,
}

impl<R: Read> MessageBoundaryReader<R> {
    fn new(inner: R) -> Self {
        MessageBoundaryReader {
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

    /// Skips all remaining bytes in the current message, possibly reaching EOF. After calling this,
    /// either `self.eof()` or `self.eom()` will be true.
    fn skip_message(&mut self) -> io::Result<()> {
        loop {
            let ready = self.fill_buf()?.len();
            if ready == 0 {
                break;
            }
            self.consume(ready);
        }
        Ok(())
    }
}

impl<R: Read> Read for MessageBoundaryReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let available = self.fill_buf()?;
        let copied = available.len().min(buf.len());
        (&mut buf[..copied]).copy_from_slice(&available[..copied]);
        self.consume(copied);
        Ok(copied)
    }
}

impl<R: Read> BufRead for MessageBoundaryReader<R> {
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
            // the index returned by memmem::find is relative to the start of the slice
            let absolute_idx = self.ready_start + newline_idx + 1;
            self.ready_end = absolute_idx;
            self.next_message_start = Some(absolute_idx);
        } else {
            self.ready_end = self.held_back;
        }

        // search for `^>+From ` and erase one `>`, doing a copy within
        if let Some(newline_off) = memmem::find(&self.buffer[self.ready_start..self.ready_end], b"\n>") {
            let first_lt_idx = self.ready_start + newline_off + 1;
            if let Some(from_off) = memmem::find(&self.buffer[first_lt_idx..self.ready_end], b"From ") {
                let f_idx = first_lt_idx + from_off;
                if self.buffer[first_lt_idx..f_idx].iter().all(|&b| b == b'>') {
                    // this is expensive - possibly O(n) - but very rare
                    self.buffer.remove(first_lt_idx);
                    self.buffer.push(0);
                    self.ready_end -= 1;
                    self.held_back -= 1;
                    self.buffer_end -= 1;
                    self.next_message_start = self.next_message_start.map(|x| x - 1);
                }
            }
        }

        Ok(&self.buffer[self.ready_start..self.ready_end])
    }

    fn consume(&mut self, amt: usize) {
        assert!(amt <= (self.ready_end - self.ready_start));
        self.ready_start += amt;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn dequoting() {
        let input_lines: Vec<&[u8]> = vec![
            b"From first@email.com",
            b">From some other dude",
            b"Here's a line of text",
            b"From second@email.com",
            b"Another line of text",
            b">From yet another email address",
            b"With more lines of text",
        ];
        let input: Vec<u8> = input_lines.join(&b"\r\n"[..]);
        let mut reader = MboxReader::new(input.as_slice());

        let first_message: io::Result<Vec<String>> = reader.next().unwrap().unwrap().lines().collect();
        let first_message = first_message.unwrap();
        assert_eq!(first_message.len(), 3);
        assert_eq!(first_message[0].as_bytes(), input_lines[0]);
        assert_eq!(first_message[1].as_bytes(), &input_lines[1][1..]);
        assert_eq!(first_message[2].as_bytes(), input_lines[2]);

        let second_message: io::Result<Vec<String>> = reader.next().unwrap().unwrap().lines().collect();
        let second_message = second_message.unwrap();
        assert_eq!(second_message.len(), 4);
        assert_eq!(second_message[0].as_bytes(), input_lines[3]);
        assert_eq!(second_message[1].as_bytes(), input_lines[4]);
        assert_eq!(second_message[2].as_bytes(), &input_lines[5][1..]);
        assert_eq!(second_message[3].as_bytes(), input_lines[6]);
        assert!(reader.next().unwrap().is_none());
    }

    #[test]
    fn really_deep_quotes() {
        let mut input: Vec<u8> = vec![0; DEFAULT_CAPACITY - 5];
        input.extend_from_slice(b"\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>From Alice");
        let reader = MessageBoundaryReader::new(input.as_slice());
        assert_eq!(reader.lines().last().unwrap().unwrap(), ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>From Alice");
    }

    #[test]
    fn one_byte_reads() {
        // read one byte at a time and check it behaves properly
        let input = b"From line1\nFrom line2";
        let mut reader = MessageBoundaryReader::new(input.as_slice());
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
        let input = b"From line1\r\nFrom line2\r\n";
        let mut reader = MessageBoundaryReader::new(input.as_slice());
        let mut line = String::new();
        assert_eq!(reader.read_to_string(&mut line).unwrap(), 12);
        assert!(reader.eom());
        assert_eq!(line, "From line1\r\n");
        reader.reset_eom();
        line.clear();
        assert_eq!(reader.read_to_string(&mut line).unwrap(), 12);
        assert!(reader.eof().unwrap());
        assert_eq!(line, "From line2\r\n");
    }

    #[test]
    fn exact_read_sizes() {
        // read the exact number of bytes in the first line, then make sure it returns 0 bytes, then the next line
        let input = b"From line1\nFrom line2";
        let mut reader = MessageBoundaryReader::new(input.as_slice());
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
                let mut reader = MessageBoundaryReader::new(input.as_slice());
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
    fn mbox_reader() -> io::Result<()> {
        let raw_messages = [
            b"From test1\ntest1\n",
            b"From test2\ntest2\n",
            b"From test3\ntest3\n",
        ];
        let input: Vec<u8> = raw_messages
            .iter()
            .map(|&x| x.iter().copied())
            .flatten()
            .collect();
        let mut reader = MboxReader::new(input.as_slice());
        let mut messages: Vec<String> = Vec::new();
        while let Some(mut item) = reader.next()? {
            let mut msg = String::new();
            assert_eq!(item.read_to_string(&mut msg)?, 17);
            messages.push(msg);
        }
        let message_bytes: Vec<&[u8]> = messages.iter().map(|x| x.as_bytes()).collect();
        assert_eq!(&raw_messages[..], message_bytes);
        Ok(())
    }
}

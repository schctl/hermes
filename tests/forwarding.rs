use std::sync::Arc;

use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use hermes::link::Link;

struct ChannelLink {
    rx: Receiver<u8>,
    tx: Sender<u8>
}

impl<'a, const T: usize, const N: usize> Link for ChannelLink<'a, T, N> {
    fn read(&mut self, buf: &mut [u8]) -> nb::Result<usize, ()> {
        if self.rx.peek().is_none() {
            return Err(nb::Error::WouldBlock);
        }

        let mut written = 0;

        while written < buf.len() {
            if let Some(byte) = self.rx.dequeue() {
                buf[written] = byte;
                written += 1;
            } else {
                break;
            }
        }

        Ok(written)
    }

    fn write(&mut self, buf: &[u8]) -> nb::Result<usize, ()> {
        if !self.tx.ready() {
            return Err(nb::Error::WouldBlock);
        }

        for (n, byte) in buf.iter().enumerate() {
            if self.tx.enqueue(*byte).is_err() {
                return Ok(n);
            }
        }

        Ok(buf.len())
    }
}

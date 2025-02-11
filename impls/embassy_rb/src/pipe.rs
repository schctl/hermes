use hermes::link::Link;

use crate::SharedRingBuffer;

/// Bi-directional shared memory link.
///
/// Occupies 2N bytes on the stack.
#[derive(Debug, Clone)]
pub struct Pipe<const N: usize> {
    received: SharedRingBuffer<N>,
    send: SharedRingBuffer<N>,
}

impl<const N: usize> Pipe<N> {
    pub fn new() -> Self {
        Self {
            received: SharedRingBuffer::new(),
            send: SharedRingBuffer::new(),
        }
    }

    pub fn flip(self) -> Self {
        Self {
            received: self.send,
            send: self.received,
        }
    }

    pub fn split(self) -> (SharedRingBuffer<N>, SharedRingBuffer<N>) {
        (self.send, self.received)
    }
}

impl<const N: usize> Link for Pipe<N> {
    fn read(&mut self, buf: &mut [u8]) -> nb::Result<usize, ()> {
        self.received.read(buf)
    }

    fn write(&mut self, buf: &[u8]) -> nb::Result<usize, ()> {
        self.send.write(buf)
    }
}

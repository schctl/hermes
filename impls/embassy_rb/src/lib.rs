#![no_std]

extern crate alloc;

use alloc::sync::Arc;

use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::mutex::Mutex;
use hermes::link::Link;
use ringbuffer::{ConstGenericRingBuffer, RingBuffer};

type NoopMutex<M> = Mutex<NoopRawMutex, M>;

pub mod pipe;

/// A shared memory region that acts as a hermes link.
#[derive(Debug, Clone)]
pub struct SharedRingBuffer<const N: usize> {
    inner: Arc<NoopMutex<ConstGenericRingBuffer<u8, N>>>,
}

impl<const N: usize> SharedRingBuffer<N> {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(NoopMutex::new(ConstGenericRingBuffer::new())),
        }
    }
}

impl<const N: usize> Link for SharedRingBuffer<N> {
    fn read(&mut self, buf: &mut [u8]) -> nb::Result<usize, ()> {
        if let Ok(mut received) = self.inner.try_lock() {
            let len = received.len().min(buf.len());

            if len == 0 {
                return Err(nb::Error::WouldBlock);
            }

            // hopefully the compiler optimizes this out
            for (n, b) in received.drain().take(len).enumerate() {
                buf[n] = b;
            }
            return Ok(len);
        }

        Err(nb::Error::WouldBlock)
    }

    fn write(&mut self, buf: &[u8]) -> nb::Result<usize, ()> {
        if let Ok(mut send) = self.inner.try_lock() {
            let len = send.capacity().min(buf.len());

            if len == 0 {
                return Err(nb::Error::WouldBlock);
            }

            // hopefully the compiler optimizes this out as well
            for b in buf.iter().take(len) {
                send.enqueue(*b);
            }
            return Ok(len);
        }

        Err(nb::Error::WouldBlock)
    }
}

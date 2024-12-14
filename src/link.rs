use core::cmp::min;
use core::future::Future;
use core::mem::transmute;
use core::pin::Pin;
use core::task::{Context, Poll};

use heapless::spsc::Queue;
use heapless::Vec;
use postcard::accumulator::{CobsAccumulator, FeedResult};

use crate::packet::Packet;

pub trait Link {
    /// Read as many bytes as possible into `buf`.
    ///
    /// If no bytes are available, `nb::WouldBlock` is returned.
    fn read(&mut self, buf: &mut [u8]) -> nb::Result<usize, ()>;

    /// Write as many bytes as possible from `buf`.
    ///
    /// If no bytes are written, `nb::WouldBlock` is returned.
    fn write(&mut self, buf: &[u8]) -> nb::Result<usize, ()>;
}

/// Shared vector-like queue link.
impl<const T: usize> Link for Vec<u8, T> {
    fn read(&mut self, buf: &mut [u8]) -> nb::Result<usize, ()> {
        let len = self.len();
        let bytes = min(len, buf.len());

        if bytes == 0 {
            return Err(nb::Error::WouldBlock);
        }

        buf.split_at_mut(bytes).0.copy_from_slice(&self[0..bytes]);
        self.copy_within(bytes..len, 0);
        self.truncate(len - bytes);

        Ok(bytes)
    }

    fn write(&mut self, buf: &[u8]) -> nb::Result<usize, ()> {
        let bytes = min(self.capacity() - self.len(), buf.len());

        if bytes == 0 {
            return Err(nb::Error::WouldBlock);
        }

        self.extend_from_slice(&buf[0..bytes]).unwrap();
        Ok(bytes)
    }
}

/// spsc queue pair link.
pub struct ChannelLink<'a, const T: usize, const N: usize> {
    rx: heapless::spsc::Consumer<'a, u8, T>,
    tx: heapless::spsc::Producer<'a, u8, N>,
}

impl<'a, const T: usize, const N: usize> ChannelLink<'a, T, N> {
    pub fn new(
        queue_1: &'a mut Queue<u8, T>,
        queue_2: &'a mut Queue<u8, N>,
    ) -> (ChannelLink<'a, T, N>, ChannelLink<'a, N, T>) {
        let split_1 = queue_1.split();
        let split_2 = queue_2.split();

        (
            ChannelLink {
                rx: split_1.1,
                tx: split_2.0,
            },
            ChannelLink {
                rx: split_2.1,
                tx: split_1.0,
            },
        )
    }
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

        for (n, byte) in buf.into_iter().enumerate() {
            if let Err(_) = self.tx.enqueue(*byte) {
                return Ok(n);
            }
        }

        Ok(buf.len())
    }
}

pub struct LinkedNode<'l> {
    link: &'l mut dyn Link,
    accumulator: CobsAccumulator<256>,
    look_ahead: [u8; 128],
    look_ahead_idx: usize,
}

impl<'l> LinkedNode<'l> {
    pub fn new(link: &'l mut dyn Link) -> Self {
        Self {
            link,
            accumulator: CobsAccumulator::new(),
            look_ahead: [0; 128],
            look_ahead_idx: 0,
        }
    }

    fn _read_packet_intl<'a>(&'a mut self) -> nb::Result<Packet<'a>, ()> {
        let ret = self.link.read(&mut self.look_ahead[self.look_ahead_idx..]);

        let bytes = match ret {
            Ok(bytes) => bytes + self.look_ahead_idx,
            Err(nb::Error::WouldBlock) => {
                if self.look_ahead_idx == 0 {
                    return Err(nb::Error::WouldBlock);
                } else {
                    self.look_ahead_idx
                }
            }
            _ => {
                return Err(nb::Error::Other(()));
            }
        };

        match self.accumulator.feed_ref(&self.look_ahead[..bytes]) {
            FeedResult::Consumed => {
                self.look_ahead_idx = 0;
            }
            FeedResult::DeserError(remaining) => {
                let rem_idx = bytes - remaining.len();
                self.look_ahead_idx = remaining.len();
                self.look_ahead.copy_within(rem_idx.., 0);
                self.look_ahead.split_at_mut(self.look_ahead_idx).1.fill(0);
            }
            FeedResult::OverFull(remaining) => {
                let rem_idx = bytes - remaining.len();
                self.look_ahead_idx = remaining.len();
                self.look_ahead.copy_within(rem_idx.., 0);
                self.look_ahead.split_at_mut(self.look_ahead_idx).1.fill(0);
            }
            FeedResult::Success { data, remaining } => {
                let rem_idx = bytes - remaining.len();
                self.look_ahead_idx = remaining.len();
                self.look_ahead.copy_within(rem_idx.., 0);
                self.look_ahead.split_at_mut(self.look_ahead_idx).1.fill(0);

                return Ok(data);
            }
        }

        Err(nb::Error::WouldBlock)
    }

    /// Fully block and write one packet into the network.
    pub fn write_packet(&mut self, packet: &Packet) -> nb::Result<(), ()> {
        let bytes = postcard::to_vec_cobs::<Packet, 256>(&packet).unwrap();

        let mut written = 0;

        while written < bytes.len() {
            written += self.link.write(&bytes)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::packet::tests::TEST_PACKETS;
    use crate::packet::Packet;

    /// Test that channel link can correctly send and receive bytes.
    #[test]
    fn test_channel_link() {
        // FIXME: channel driver queue size is kind of funky

        let mut buffer_1 = Queue::<u8, 129>::new();
        let mut buffer_2 = Queue::<u8, 129>::new();

        let (mut link_1, mut link_2) = ChannelLink::new(&mut buffer_1, &mut buffer_2);

        let written = link_1.write(&[2; 128]).unwrap();

        assert_eq!(written, 128);

        let mut buf = [0; 128];
        link_2.read(&mut buf).unwrap();

        assert_eq!(buf, [2; 128]);
    }

    /// Check that the internal accumulator can correctly handle receiving a stream of bytes
    /// and decode it into corresponding packets.
    #[test]
    fn test_link_accumulation() {
        let mut buffer = Vec::<u8, 1024>::new();

        for packet in TEST_PACKETS.into_iter() {
            buffer
                .write(&postcard::to_vec_cobs::<Packet, 256>(&packet).unwrap())
                .unwrap();
        }

        let mut linked_node = LinkedNode::new(&mut buffer);

        let mut idx = 0;

        loop {
            match linked_node._read_packet_intl() {
                Ok(packet) => {
                    assert_eq!(packet, TEST_PACKETS[idx]);
                    idx += 1;
                }
                Err(_) => {
                    if idx == 3 {
                        break;
                    }
                }
            }
        }
    }
}

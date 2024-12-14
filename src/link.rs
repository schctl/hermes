use core::cmp::min;

use heapless::spsc::Queue;
use heapless::Vec;
use postcard::accumulator::{CobsAccumulator, FeedResult};

use crate::packet::Packet;

pub trait Link {
    fn read(&mut self, buf: &mut [u8]) -> nb::Result<usize, ()>;
    fn write(&mut self, buf: &[u8]) -> nb::Result<usize, ()>;
}

/// Link implemented as a stack.
impl<const T: usize> Link for Vec<u8, T> {
    fn read(&mut self, buf: &mut [u8]) -> nb::Result<usize, ()> {
        let len = self.len();
        let bytes = min(len, buf.len());

        if bytes == 0 {
            return Err(nb::Error::WouldBlock);
        }

        buf.split_at_mut(bytes).0.copy_from_slice(&self[0..bytes]);
        self.copy_within(bytes..len, 0);
        unsafe { self.set_len(len - bytes) };
        Ok(bytes)
    }

    fn write(&mut self, buf: &[u8]) -> nb::Result<usize, ()> {
        let bytes = min(self.capacity() - self.len(), buf.len());
        self.extend_from_slice(&buf[0..bytes]).unwrap();
        Ok(bytes)
    }
}

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
}

impl<'l> LinkedNode<'l> {
    pub fn new(link: &'l mut dyn Link) -> Self {
        Self {
            link,
            accumulator: CobsAccumulator::new(),
        }
    }

    fn _accumulate<'a>(
        &mut self,
        mut f: impl FnMut(Packet),
        read_buf: &'a [u8],
    ) -> Option<&'a [u8]> {
        match self.accumulator.feed_ref(&read_buf) {
            FeedResult::Consumed => (),
            FeedResult::DeserError(remaining) => {
                if !remaining.is_empty() {
                    return Some(remaining);
                }
            }
            FeedResult::OverFull(remaining) => {
                if !remaining.is_empty() {
                    return Some(remaining);
                }
            }
            FeedResult::Success { data, remaining } => {
                (f)(data);

                if !remaining.is_empty() {
                    return Some(remaining);
                }
            }
        }

        None
    }

    /// Try to read and process as many packets as possible.
    pub fn try_read_packets(&mut self, mut f: impl FnMut(Packet)) {
        let mut read_buf = [0; 16];

        while let Ok(mut bytes) = self.link.read(&mut read_buf) {
            while let Some(rem) = self._accumulate(&mut f, &read_buf[..bytes]) {
                let rem_idx = bytes - rem.len();
                let bytes_new = rem.len();
                read_buf.copy_within(rem_idx..bytes, 0);
                bytes = bytes_new;
            }
        }
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

        linked_node.try_read_packets(|p| {
            assert_eq!(p, TEST_PACKETS[idx], "index {idx} failed");
            idx += 1;
        });

        assert_eq!(idx, TEST_PACKETS.len());
    }
}

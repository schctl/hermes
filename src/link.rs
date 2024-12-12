use core::fmt::Debug;

use heapless::Vec;
use postcard::accumulator::{CobsAccumulator, FeedResult};

use crate::packet::Packet;

pub trait Link: Debug {
    fn read(&mut self) -> nb::Result<u8, ()>;
    fn write(&mut self, byte: u8) -> nb::Result<(), u8>;
}

/// Link implemented as a stack.
impl<const T: usize> Link for Vec<u8, T> {
    fn read(&mut self) -> nb::Result<u8, ()> {
        self.pop().ok_or(nb::Error::Other(()))
    }

    fn write(&mut self, byte: u8) -> nb::Result<(), u8> {
        self.push(byte).map_err(nb::Error::Other)
    }
}

pub struct LinkedNode<'l> {
    link: &'l mut dyn Link,
    accumulator: CobsAccumulator<256>,
    read_buf: Vec<u8, 16>,
}

impl<'l> LinkedNode<'l> {
    pub fn new(link: &'l mut dyn Link) -> Self {
        Self {
            link,
            accumulator: CobsAccumulator::new(),
            read_buf: Vec::new(),
        }
    }

    fn _accumulate(&mut self, mut f: impl FnMut(Packet)) {
        let mut remaining = Vec::<u8, 16>::new();

        match self.accumulator.feed_ref(&self.read_buf) {
            FeedResult::Consumed => (),
            FeedResult::DeserError(rem) => {
                remaining.extend_from_slice(rem).unwrap();
                // error!("unable to process byte sequence")
            }
            FeedResult::OverFull(rem) => {
                remaining.extend_from_slice(rem).unwrap();
                // error!("accumulator buffer overflow")
            }
            FeedResult::Success {
                data,
                remaining: rem,
            } => {
                remaining.extend_from_slice(rem).unwrap();
                (f)(data);
            }
        }

        self.read_buf = remaining.clone();
    }

    pub fn read_packet(&mut self, mut f: impl FnMut(Packet)) {
        while let Ok(byte) = self.link.read() {
            if self.read_buf.is_full() {
                self._accumulate(&mut f);
            }

            self.read_buf.push(byte).unwrap();
        }

        self._accumulate(&mut f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::node;
    use crate::packet::{Message, Packet};
    use crate::topic;

    use crate::packet::tests::TEST_PACKETS;

    /// Check that the internal accumulator can correctly handle receiving a stream of bytes
    /// and decode it into corresponding packets.
    #[test]
    fn test_link_accumulation() {
        let mut buffer = Vec::<u8, 1024>::new();

        for packet in TEST_PACKETS.into_iter() {
            buffer.extend(postcard::to_vec_cobs::<Packet, 256>(&packet).unwrap());
        }

        buffer.reverse();

        let mut linked_node = LinkedNode::new(&mut buffer);

        let mut idx = 0;

        linked_node.read_packet(|p| {
            assert_eq!(p, TEST_PACKETS[idx], "index {idx} failed");
            idx += 1;
        });

        assert_eq!(idx, TEST_PACKETS.len());
    }
}

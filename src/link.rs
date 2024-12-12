use heapless::spsc::Queue;
use heapless::Vec;
use postcard::accumulator::{CobsAccumulator, FeedResult};

use crate::packet::Packet;

pub trait Link {
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
    fn read(&mut self) -> nb::Result<u8, ()> {
        self.rx.dequeue().ok_or(nb::Error::WouldBlock)
    }

    fn write(&mut self, byte: u8) -> nb::Result<(), u8> {
        self.tx.enqueue(byte).map_err(|e| nb::Error::Other(e))
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

    // TODO: make this nb
    pub fn read_packet(&mut self, mut f: impl FnMut(Packet)) {
        while let Ok(byte) = self.link.read() {
            if self.read_buf.is_full() {
                self._accumulate(&mut f);
            }

            self.read_buf.push(byte).unwrap();
        }

        self._accumulate(&mut f);
    }

    pub fn write_packet(&mut self, packet: &Packet) {
        let bytes = postcard::to_vec_cobs::<Packet, 256>(&packet).unwrap();

        for byte in bytes {
            self.link.write(byte).unwrap();
        }
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

        let mut buffer_1 = Queue::<u8, 16>::new();
        let mut buffer_2 = Queue::<u8, 16>::new();

        let (mut link_1, mut link_2) = ChannelLink::new(&mut buffer_1, &mut buffer_2);

        for byte in 0..12 {
            link_1.write(byte).unwrap();
        }

        for byte in 0..12 {
            assert_eq!(link_2.read().unwrap(), byte);
        }
    }

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

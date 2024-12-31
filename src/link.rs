use core::cmp::min;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use heapless::spsc::Queue;
use heapless::Vec;
use postcard::accumulator::{CobsAccumulator, FeedResult};

use crate::packet::Packet;

pub trait Link: Send {
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

impl<const T: usize, const N: usize> Link for ChannelLink<'_, T, N> {
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

    fn _read_packet_intl(&mut self) -> nb::Result<Packet, ()> {
        let ret = self.link.read(&mut self.look_ahead[self.look_ahead_idx..]);

        let bytes = match ret {
            Ok(bytes) => bytes + self.look_ahead_idx,
            Err(nb::Error::WouldBlock) => {
                if self.look_ahead_idx == 0 {
                    return Err(nb::Error::WouldBlock);
                }

                self.look_ahead_idx
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
    pub fn write_packet<'a>(
        &'a mut self,
        packet: &Packet,
    ) -> postcard::Result<WriteFuture<'a, 'l>> {
        let buffer = postcard::to_vec_cobs::<Packet, 256>(packet)?;

        Ok(WriteFuture {
            node: self,
            buffer,
            written: 0,
        })
    }

    pub fn dummy_write(&mut self) -> postcard::Result<WriteFuture<'_, 'l>> {
        Ok(WriteFuture::dummy(self))
    }

    /// Fully block and read one packet into the netwwork.
    pub fn read_packet(&mut self) -> ReadFuture<'_, 'l> {
        ReadFuture { node: self }
    }
}

/// Future which attemps to read incoming packets.
///
/// This future is safely cancellable.
pub struct ReadFuture<'n, 'l> {
    node: &'n mut LinkedNode<'l>,
}

impl<'n, 'l> Future for ReadFuture<'n, 'l> {
    type Output = Packet<'n>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.get_mut();
        let this_ptr: *mut LinkedNode<'l> = this.node;
        // SAFETY: I don't know what the FUCK is going on here
        // This should be fine though, since the lifetime of the future should be less than that
        // of the node. The only reason we can't otherwise bind the output lifetime is trait restrictions.
        let node: &'n mut LinkedNode<'l> = unsafe { &mut *this_ptr };

        match node._read_packet_intl() {
            Ok(packet) => Poll::Ready(packet),
            Err(_) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

/// Write a serialized packet into the network.
///
/// This future is NOT safely cancellable.
pub struct WriteFuture<'n, 'l> {
    node: &'n mut LinkedNode<'l>,
    buffer: Vec<u8, 256>,
    written: usize,
}

impl<'n, 'l> WriteFuture<'n, 'l> {
    pub(crate) fn dummy(node: &'n mut LinkedNode<'l>) -> Self {
        Self {
            node,
            buffer: Vec::new(),
            written: 0,
        }
    }
}

impl Future for WriteFuture<'_, '_> {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        // FIXME: cancelling this future should somehow indicate clearly to the network that the
        // transmitted packet is incomplete by writing the sentinel byte. otherwise the adjacent
        // packet will also be invalidated and dropped.

        let this = self.get_mut();

        if this.written == this.buffer.len() {
            return Poll::Ready(true);
        }

        match this.node.link.write(&this.buffer[this.written..]) {
            Ok(bytes) => {
                if bytes < this.buffer.len() {
                    this.written += bytes;
                    Poll::Pending
                } else {
                    Poll::Ready(true)
                }
            }
            Err(nb::Error::WouldBlock) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(_) => Poll::Ready(false),
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

    #[tokio::test]
    async fn test_link_accumulation_async() {
        let mut buffer = Vec::<u8, 1024>::new();

        for packet in TEST_PACKETS.into_iter() {
            buffer
                .write(&postcard::to_vec_cobs::<Packet, 256>(&packet).unwrap())
                .unwrap();
        }

        let mut linked_node = LinkedNode::new(&mut buffer);

        for test_packet in TEST_PACKETS {
            let packet = linked_node.read_packet().await;
            assert_eq!(packet, test_packet);
        }
    }
}

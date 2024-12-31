use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll};

use fastrand::Rng;
use heapless::Vec;

use crate::link::{Link, LinkedNode, ReadFuture, WriteFuture};
use crate::packet::{self, Packet};
use crate::topic;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub enum Id {
    Id(u16),
    Anonymous,
}

pub struct Node<'l, const N: usize> {
    id: Id,
    links: [LinkedNode<'l>; N],
}

impl<'l, const N: usize> Node<'l, N> {
    pub fn new(id: Id, links: [LinkedNode<'l>; N]) -> Self {
        Self { id, links }
    }

    pub fn new_with_links(id: Id, links: [&'l mut dyn Link; N]) -> Self {
        Self::new(id, links.map(LinkedNode::new))
    }

    pub fn publish(&mut self, message: topic::Message) -> postcard::Result<PubFuture<'_, 'l, N>> {
        self.publish_except(message, &[])
    }

    pub fn publish_except(
        &mut self,
        message: topic::Message,
        except: &[usize],
    ) -> postcard::Result<PubFuture<'_, 'l, N>> {
        debug_assert!(message.data.len() <= 256); // FIXME: arbitrary limit

        let packet = Packet {
            origin: self.id,
            message: packet::Message::Publish(message),
        };

        let collect_futures = self
            .links
            .iter_mut()
            .enumerate()
            .map(|(n, link)| {
                if except.contains(&n) {
                    link.dummy_write()
                } else {
                    link.write_packet(&packet)
                }
            })
            .collect::<postcard::Result<Vec<WriteFuture, N>>>()?;

        Ok(PubFuture {
            futures: unsafe { collect_futures.into_array().unwrap_unchecked() },
            rng: Rng::with_seed(N as u64),
        })
    }

    pub fn wait_packet(&mut self) -> WaitPacketFuture<'_, 'l, N> {
        let collect_futures = self
            .links
            .iter_mut()
            .map(|link| link.read_packet())
            .collect::<Vec<ReadFuture, N>>();

        WaitPacketFuture {
            futures: unsafe { collect_futures.into_array().unwrap_unchecked() },
            rng: Rng::with_seed(N as u64),
        }
    }

    pub async fn run<T>(&mut self, mut callback: impl FnMut(topic::Message) -> T, cancel: AtomicBool)
    where
        T: Future<Output = ()>,
    {
        loop {
            if cancel.load(Ordering::Relaxed) {
                return;
            }

            let (idx, packet) = self.wait_packet().await;
            match packet.message {
                packet::Message::Publish(message) => {
                    let data_clone = Vec::<_, 256>::from_slice(message.data).unwrap();
                    let message = topic::Message {
                        id: message.id,
                        data: &data_clone
                    };

                    // FIXME: should this be zip?
                    // how to handle cases where publish_except hangs?
                    // in the future when we have QoS contracts, we'd ideally want to be able to
                    // cancel publish_except if it takes too long and carry on with processing packets.
                    futures_lite::future::zip(
                        (callback)(message),
                        self.publish_except(message, &[idx]).unwrap(),
                    ).await;
                }
            }
        }
    }
}

// This should be optimized out, hopefully.
fn indices<const N: usize>() -> [usize; N] {
    (0..N).collect::<Vec<usize, N>>().into_array().unwrap()
}

pub struct PubFuture<'n, 'l, const N: usize> {
    futures: [WriteFuture<'n, 'l>; N],
    rng: Rng,
}

impl<'n, 'l, const N: usize> Future for PubFuture<'n, 'l, N> {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let mut done = Some(true);

        let mut indices = indices::<N>();
        this.rng.shuffle(&mut indices);

        for idx in indices {
            if let Poll::Ready(success) = Pin::new(&mut this.futures[idx]).poll(cx) {
                done = done.map(|s| s && success);
                break;
            }
        }

        done.map_or(Poll::Pending, Poll::Ready)
    }
}

pub struct WaitPacketFuture<'n, 'l, const N: usize> {
    futures: [ReadFuture<'n, 'l>; N],
    rng: Rng,
}

impl<'n, 'l, const N: usize> Future for WaitPacketFuture<'n, 'l, N> {
    type Output = (usize, Packet<'n>);

    #[allow(irrefutable_let_patterns)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let mut indices = indices::<N>();
        this.rng.shuffle(&mut indices);

        for idx in indices {
            let poll_result = Pin::new(&mut this.futures[idx]).poll(cx);

            if poll_result.is_ready() {
                return poll_result.map(|p| (idx, p));
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use heapless::spsc::Queue;
    use tokio::sync::Arc;

    use super::*;
    use crate::link::ChannelLink;

    const MESSAGES: [topic::Message; 4] = [
        topic::Message {
            id: 2,
            data: &[0, 1, 2, 3],
        },
        topic::Message {
            id: 2,
            data: &[4, 5, 6, 7],
        },
        topic::Message {
            id: 3,
            data: &[8, 9, 10, 11],
        },
        topic::Message {
            id: 2,
            data: &[12, 13, 14, 15],
        },
    ];

    #[tokio::test]
    async fn test_node_pub_sub_async() {
        let mut buffer_1 = Queue::<u8, 128>::new();
        let mut buffer_2 = Queue::<u8, 128>::new();

        let (mut link_1, mut link_2) = ChannelLink::new(&mut buffer_1, &mut buffer_2);

        let mut node_1 = Node::new_with_links(Id::Id(5), [&mut link_1]);
        let mut node_2 = Node::new_with_links(Id::Id(6), [&mut link_2]);

        for m in MESSAGES {
            node_1.publish(m).unwrap().await;
        }

        for idx in 0..3 {
            let message = node_2.wait_packet().await;
            assert_eq!(message.1.message, packet::Message::Publish(MESSAGES[idx]));
        }
    }

    #[tokio::test]
    async fn test_node_pub_sub_multicon_async() {
        let mut buffer_1 = Queue::<u8, 128>::new();
        let mut buffer_2 = Queue::<u8, 128>::new();

        let (mut link_1, mut link_2) = ChannelLink::new(&mut buffer_1, &mut buffer_2);

        let mut buffer_3 = Queue::<u8, 128>::new();
        let mut buffer_4 = Queue::<u8, 128>::new();

        let (mut link_3, mut link_4) = ChannelLink::new(&mut buffer_3, &mut buffer_4);

        let mut node_1 = Node::new_with_links(Id::Id(5), [&mut link_1]);
        let mut node_2 = Node::new_with_links(Id::Id(6), [&mut link_3]);
        let mut node_3 = Node::new_with_links(Id::Id(7), [&mut link_2, &mut link_4]);

        node_1.publish(MESSAGES[1]).unwrap().await;
        assert_eq!(
            node_3.wait_packet().await.1.message,
            packet::Message::Publish(MESSAGES[1])
        );

        node_2.publish(MESSAGES[3]).unwrap().await;
        assert_eq!(
            node_3.wait_packet().await.1.message,
            packet::Message::Publish(MESSAGES[3])
        );
    }
}

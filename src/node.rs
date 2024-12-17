use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use fastrand::Rng;
use heapless::{FnvIndexSet, Vec};

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
    // FIXME: all this can probably be more memory efficient
    links: [LinkedNode<'l>; N],
    subscriptions: FnvIndexSet<topic::Id, 16>,
}

impl<'l, const N: usize> Node<'l, N> {
    pub fn new(id: Id, links: [LinkedNode<'l>; N]) -> Self {
        Self {
            id,
            links,
            subscriptions: FnvIndexSet::new(),
        }
    }

    pub fn new_with_links(id: Id, links: [&'l mut dyn Link; N]) -> Self {
        Self::new(id, links.map(LinkedNode::new))
    }

    pub fn subscribe(&mut self, id: topic::Id) -> bool {
        self.subscriptions.insert(id).is_ok()
    }

    pub fn unsubscribe(&mut self, id: topic::Id) -> bool {
        self.subscriptions.remove(&id)
    }

    pub fn publish<'n>(
        &'n mut self,
        message: topic::Message<'n>
    ) -> postcard::Result<PubFuture<'n, 'l, N>> {
        debug_assert!(message.data.len() <= 256); // FIXME: arbitrary limit

        let packet = Packet {
            origin: self.id,
            message: packet::Message::Publish(message),
        };

        let collect_futures = self
            .links
            .iter_mut()
            .map(|link| link.write_packet(&packet))
            .collect::<postcard::Result<Vec<WriteFuture, N>>>()?;

        Ok(PubFuture {
            futures: unsafe { collect_futures.into_array().unwrap_unchecked() },
            rng: Rng::with_seed(N as u64),
        })
    }

    pub fn wait_subscription<'n>(&'n mut self) -> WaitSubFuture<'n, 'l, N> {
        let collect_futures = self
            .links
            .iter_mut()
            .map(|link| link.read_packet())
            .collect::<Vec<ReadFuture, N>>();

        WaitSubFuture {
            subscriptions: &self.subscriptions,
            futures: unsafe { collect_futures.into_array().unwrap_unchecked() },
            rng: Rng::with_seed(N as u64),
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

        for idx in indices.into_iter() {
            if let Poll::Ready(success) = Pin::new(&mut this.futures[idx]).poll(cx) {
                done = done.map(|s| s && success);
                break;
            }
        }

        done.map_or(Poll::Pending, Poll::Ready)
    }
}

pub struct WaitSubFuture<'n, 'l, const N: usize> {
    subscriptions: &'n FnvIndexSet<topic::Id, 16>,
    futures: [ReadFuture<'n, 'l>; N],
    rng: Rng,
}

impl<'n, 'l, const N: usize> Future for WaitSubFuture<'n, 'l, N> {
    type Output = topic::Message<'n>;

    #[allow(irrefutable_let_patterns)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let mut indices = indices::<N>();
        this.rng.shuffle(&mut indices);

        for idx in indices.into_iter() {
            if let Poll::Ready(packet) = Pin::new(&mut this.futures[idx]).poll(cx) {
                // FIXME: this might not be future proof. when we add new packet handlers, we'll need
                // to be able to handle them here generically.
                if let packet::Message::Publish(message) = packet.message {
                    if this.subscriptions.contains(&message.id) {
                        return Poll::Ready(message);
                    }
                    // some other processing
                }
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use heapless::spsc::Queue;

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

        node_2.subscribe(2);
        node_2.subscribe(3);

        for m in MESSAGES {
            node_1.publish(m).unwrap().await;
        }

        for idx in 0..3 {
            let message = node_2.wait_subscription().await;
            assert_eq!(message, MESSAGES[idx]);
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

        node_3.subscribe(2);

        node_1.publish(MESSAGES[1]).unwrap().await;
        assert_eq!(node_3.wait_subscription().await, MESSAGES[1]);

        node_2.publish(MESSAGES[3]).unwrap().await;
        assert_eq!(node_3.wait_subscription().await, MESSAGES[3]);
    }
}

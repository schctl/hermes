use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use fastrand::Rng;
use heapless::{FnvIndexSet, Vec};

use crate::link::{Link, LinkedNode, WriteFuture};
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
        id: topic::Id,
        data: &'n [u8],
    ) -> postcard::Result<PubFuture<'n, 'l, N>> {
        debug_assert!(data.len() <= 256); // FIXME: arbitrary limit

        let packet = Packet {
            origin: self.id,
            message: packet::Message::Publish(topic::Message { id, data }),
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

    pub async fn wait_subscription(&self) -> topic::Message {
        todo!()
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
            node_1.publish(m.id, m.data).unwrap().await;
        }

        // for idx in 0..3 {
        //     let message = node_2.process_subscriptions().await;
        //     assert_eq!(message, MESSAGES[idx]);
        // }
    }
}

use core::future::Future;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll};

use embassy_futures::join::JoinArray;
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

impl From<u16> for Id {
    fn from(id: u16) -> Self {
        Self::Id(id)
    }
}

impl From<Option<u16>> for Id {
    fn from(id: Option<u16>) -> Self {
        match id {
            Some(id) => Self::Id(id),
            None => Self::Anonymous,
        }
    }
}

pub struct Node<'l, const N: usize, const Q: usize = 8> {
    id: Id,
    links: [LinkedNode<'l>; N],
}

impl<'l, const N: usize> Node<'l, N> {
    pub fn new(id: impl Into<Id>, links: [LinkedNode<'l>; N]) -> Self {
        let id = id.into();

        Self { id, links }
    }

    pub fn new_with_links(id: impl Into<Id>, links: [&'l mut dyn Link; N]) -> Self {
        Self::new(id, links.map(LinkedNode::new))
    }

    pub fn publish(
        &mut self,
        message: topic::Message,
    ) -> postcard::Result<JoinArray<WriteFuture<'_, 'l>, N>> {
        self.publish_except(message, &[])
    }

    pub fn publish_dummy(&mut self) -> postcard::Result<JoinArray<WriteFuture<'_, 'l>, N>> {
        let futures: [WriteFuture<'_, 'l>; N] = {
            let mut futures: [MaybeUninit<WriteFuture<'_, 'l>>; N] =
                [const { MaybeUninit::uninit() }; N];

            for fut in &mut futures {
                fut.write(unsafe { WriteFuture::dummy_unsafe() });
            }

            futures.map(|w| unsafe { core::mem::transmute(w) })
        };

        Ok(embassy_futures::join::join_array(futures))
    }

    pub fn publish_except(
        &mut self,
        message: topic::Message,
        except: &[usize],
    ) -> postcard::Result<JoinArray<WriteFuture<'_, 'l>, N>> {
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

        Ok(embassy_futures::join::join_array(unsafe {
            collect_futures.into_array().unwrap_unchecked()
        }))
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

    pub async fn run<T>(
        &mut self,
        mut callback: impl FnMut(topic::Message, &mut Vec<topic::Message, 8>) -> T,
        cancel: &AtomicBool,
    ) where
        T: Future<Output = ()>,
    {
        loop {
            let mut message_queue: Vec<topic::Message, 8> = Vec::new();

            if cancel.load(Ordering::Relaxed) {
                return;
            }

            let (idx, packet) = self.wait_packet().await;

            match packet.message {
                packet::Message::Publish(message) => {
                    let data_clone = Vec::<_, 256>::from_slice(message.data).unwrap();
                    let message = topic::Message {
                        id: message.id,
                        data: &data_clone,
                    };

                    // FIXME: should this be zip?
                    // how to handle cases where publish_except hangs?
                    // in the future when we have QoS contracts, we'd ideally want to be able to
                    // cancel publish_except if it takes too long and carry on with processing packets.
                    embassy_futures::join::join(
                        (callback)(message, &mut message_queue),
                        self.publish_except(message, &[idx]).unwrap(),
                    )
                    .await;
                }
            }

            for message in message_queue {
                if let Ok(tok) = self.publish(message) {
                    tok.await;
                }
            }
        }
    }
}

// This should be optimized out, hopefully.
fn indices<const N: usize>() -> [usize; N] {
    (0..N).collect::<Vec<usize, N>>().into_array().unwrap()
}

pub struct WaitPacketFuture<'n, 'l, const N: usize> {
    futures: [ReadFuture<'n, 'l>; N],
    rng: Rng,
}

impl<'n, const N: usize> Future for WaitPacketFuture<'n, '_, N> {
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

        for m in MESSAGES {
            let message = node_2.wait_packet().await;
            assert_eq!(message.1.message, packet::Message::Publish(m));
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

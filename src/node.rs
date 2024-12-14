use heapless::FnvIndexSet;
use slab::Slab;

use crate::link::{Link, LinkedNode};
use crate::packet::{self, Packet};
use crate::topic;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub enum Id {
    Id(u16),
    Anonymous,
}

pub struct Node<'l> {
    id: Id,
    // FIXME: all this can probably be more memory efficient
    links: Slab<LinkedNode<'l>>,
    subscriptions: FnvIndexSet<topic::Id, 16>,
}

impl<'l> Node<'l> {
    pub fn new(id: Id) -> Self {
        Self {
            id,
            links: Slab::with_capacity(4),
            subscriptions: FnvIndexSet::new(),
        }
    }

    pub fn add_link(&mut self, link: &'l mut dyn Link) -> usize {
        self.links.insert(LinkedNode::new(link))
    }

    pub fn remove_link(&mut self, link: usize) -> LinkedNode<'l> {
        self.links.remove(link)
    }

    pub fn subscribe(&mut self, id: topic::Id) -> bool {
        self.subscriptions.insert(id).is_ok()
    }

    pub fn unsubscribe(&mut self, id: topic::Id) -> bool {
        self.subscriptions.remove(&id)
    }

    pub fn publish(&mut self, id: topic::Id, data: &[u8]) {
        for (_, link) in self.links.iter_mut() {
            link.write_packet(&Packet {
                origin: self.id,
                message: packet::Message::Publish(topic::Message { id, data }),
            });
        }
    }

    pub fn process_subscriptions(&mut self, mut f: impl FnMut(topic::Message)) {
        //for (_, link) in self.links.iter_mut() {
        //    while let Ok(packet) = link.read_packet() {
        //        match packet.message {
        //            packet::Message::Publish(m) => {
        //                if self.subscriptions.contains(&m.id) {
        //                    (f)(m);
        //                }
        //            }
        //        }
        //    }
        //}
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

    #[test]
    fn test_node_pub_sub() {
        let mut buffer_1 = Queue::<u8, 128>::new();
        let mut buffer_2 = Queue::<u8, 128>::new();

        let (mut link_1, mut link_2) = ChannelLink::new(&mut buffer_1, &mut buffer_2);

        let mut node_1 = Node::new(Id::Id(5));
        node_1.add_link(&mut link_1);

        let mut node_2 = Node::new(Id::Id(6));
        node_2.add_link(&mut link_2);

        node_2.subscribe(2);
        node_2.subscribe(3);

        for m in MESSAGES {
            node_1.publish(m.id, m.data);
        }

        let mut idx = 0;

        node_2.process_subscriptions(|m| {
            assert_eq!(m, MESSAGES[idx]);
            idx += 1;
        });

        assert_eq!(idx, MESSAGES.len());
    }
}

// use std::sync::Arc;

use std::sync::atomic::AtomicBool;

use hermes::link::Link;
use tokio::sync::mpsc::{channel, Receiver, Sender};

struct ChannelLink {
    rx: Receiver<u8>,
    tx: Sender<u8>,
}

impl ChannelLink {
    pub fn new() -> (Self, Self) {
        let one = channel(512);
        let two = channel(512);

        (
            ChannelLink {
                rx: one.1,
                tx: two.0,
            },
            ChannelLink {
                rx: two.1,
                tx: one.0,
            },
        )
    }
}

impl Link for ChannelLink {
    fn read(&mut self, buf: &mut [u8]) -> nb::Result<usize, ()> {
        let mut written = 0;

        while written < buf.len() {
            if let Ok(byte) = self.rx.try_recv() {
                buf[written] = byte;
                written += 1;
            } else {
                break;
            }
        }

        Ok(written)
    }

    fn write(&mut self, buf: &[u8]) -> nb::Result<usize, ()> {
        for (n, byte) in buf.iter().enumerate() {
            if self.tx.try_send(*byte).is_err() {
                return Ok(n);
            }
        }

        Ok(buf.len())
    }
}

#[tokio::test]
async fn test_tokio_channel() {
    let message = hermes::Message {
        id: 0,
        data: b"Hello, world!",
    };

    let (mut link_1, mut link_2) = ChannelLink::new();

    let mut node_1 = hermes::Node::new_with_links(5, [&mut link_1]);
    let mut node_2 = hermes::Node::new_with_links(6, [&mut link_2]);

    node_1.publish(message).unwrap().await;
    assert_eq!(
        node_2.wait_packet().await.1.message,
        hermes::packet::Message::Publish(message)
    );
}

#[tokio::test]
async fn test_forwarding() {
    let message = hermes::Message {
        id: 0,
        data: b"Hello, world!",
    };

    let (mut link_1, mut link_2) = ChannelLink::new();
    let (mut link_3, mut link_4) = ChannelLink::new();

    // here, node_1 and node_3 have no direct link to each other
    // we expect node_2 to forward messages appropriately
    let mut node_1 = hermes::Node::new_with_links(5, [&mut link_1]);
    let mut node_3 = hermes::Node::new_with_links(6, [&mut link_4]);

    tokio::task::spawn(async move {
        let mut node_2 = hermes::Node::new_with_links(6, [&mut link_2, &mut link_3]);
        node_2.run(|_| async {}, AtomicBool::new(false)).await;
    });

    node_1.publish(message).unwrap().await;
    assert_eq!(
        node_3.wait_packet().await.1.message,
        hermes::packet::Message::Publish(message)
    );
}

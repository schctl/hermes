use crate::node;
use crate::topic;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub enum Message<'d> {
    /// Publish a message to a topic.
    #[serde(borrow)]
    Publish(topic::Message<'d>), // TODO: add QoS checks, fletcher16 ack/nack, etc.
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct Packet<'d> {
    origin: node::Id,

    /// Catch all message type.
    #[serde(borrow)]
    message: Message<'d>,
}

#[cfg(test)]
mod tests {
    use postcard::accumulator::{CobsAccumulator, FeedResult};

    use super::*;

    const TEST_PACKETS: [Packet<'static>; 3] = [
        Packet {
            origin: node::Id::Id(1),
            message: Message::Publish(topic::Message {
                id: 1,
                data: &[2; 128],
            }),
        },
        Packet {
            origin: node::Id::Anonymous,
            message: Message::Publish(topic::Message {
                id: 0,
                data: &[0, 0, 0, 0],
            }),
        },
        Packet {
            origin: node::Id::Anonymous,
            message: Message::Publish(topic::Message {
                id: 0,
                data: &[2, 4, 6, 8],
            }),
        },
    ];

    #[test]
    fn basic_postcard_de_ser() {
        for (idx, packet) in TEST_PACKETS.into_iter().enumerate() {
            let mut buffer = [0; 256];
            let packet_bytes = postcard::to_slice(&packet, &mut buffer).unwrap();
            let packet_de: Packet = postcard::from_bytes(&packet_bytes).unwrap();

            if packet != packet_de {
                panic!("Packet `{}` postcard to-and-fro mismatch", idx);
            }
        }
    }

    #[test]
    fn cobs_accumulator_queue() {
        let mut buffer = [0; 512];
        let mut accumulator = CobsAccumulator::<256>::new();

        let mut wrote_len = 0;

        for packet in TEST_PACKETS.into_iter() {
            let packet_len = postcard::to_slice_cobs(&packet, &mut buffer[wrote_len..]).unwrap().len();
            wrote_len += packet_len;
        }

        let mut idx = 0;

        let mut chunks = buffer.chunks(32);
        let mut chunk = chunks.next();

        loop {
            if let Some(ch) = chunk {
                match accumulator.feed_ref::<Packet>(ch) {
                    FeedResult::Consumed => (),
                    FeedResult::DeserError(e) => panic!("Deser error: {:?}", e),
                    FeedResult::OverFull(e) => panic!("Buffer filled: {:?}", e),
                    FeedResult::Success { data, remaining } => {
                        if TEST_PACKETS[idx] != data {
                            panic!("Packet `{}` postcard to-and-fro mismatch", idx);
                        }

                        chunk = Some(remaining);
                        idx += 1;

                        if idx == TEST_PACKETS.len() {
                            break;
                        }

                        continue;
                    }
                }

                chunk = chunks.next();
            }
        }
    }
}

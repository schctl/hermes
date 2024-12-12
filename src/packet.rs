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
    pub origin: node::Id,

    /// Catch all message type.
    #[serde(borrow)]
    pub message: Message<'d>,
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub const TEST_PACKETS: [Packet<'static>; 3] = [
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

    /// Ensure that multiple packet formats are correctly serializable and deserializable.
    #[test]
    fn basic_postcard_de_ser() {
        for (idx, packet) in TEST_PACKETS.into_iter().enumerate() {
            let mut buffer = [0; 256];
            let packet_bytes = postcard::to_slice_cobs(&packet, &mut buffer).unwrap();
            let packet_de: Packet = postcard::from_bytes_cobs(packet_bytes).unwrap();

            assert_eq!(packet, packet_de, "index {idx} failed");
        }
    }
}

pub type Id = u16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct Message<'d> {
    pub id: Id,
    pub data: &'d [u8],
}

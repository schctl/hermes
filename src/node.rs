use core::fmt::Debug;

use heapless::Vec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub enum Id {
    Id(u16),
    Anonymous,
}

pub trait Link: Debug {
    fn read(&mut self) -> nb::Result<u8, ()>;
    fn write(&mut self, byte: u8) -> nb::Result<(), ()>;
}

#[derive(Debug, Clone)]
pub struct Node<'l> {
    id: Id,
    links: Vec<&'l dyn Link, 16>,
}

impl<'l> Node<'l> {
    pub fn new(id: Id) -> Self {
        Self {
            id,
            links: Vec::new(),
        }
    }

    // TODO: store link by hash and provide "token" through which link can be deleted
    pub fn add_link(&mut self, link: &'l dyn Link) -> bool {
        self.links.push(link).is_ok()
    }
}

use slab::Slab;

use crate::link::{Link, LinkedNode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub enum Id {
    Id(u16),
    Anonymous,
}

pub struct Node<'l> {
    id: Id,
    links: Slab<LinkedNode<'l>>,
}

impl<'l> Node<'l> {
    pub fn new(id: Id) -> Self {
        Self {
            id,
            links: Slab::with_capacity(4),
        }
    }

    pub fn add_link(&mut self, link: &'l mut dyn Link) -> usize {
        self.links.insert(LinkedNode::new(link))
    }

    pub fn remove_link(&mut self, link: usize) -> LinkedNode<'l> {
        self.links.remove(link)
    }
}

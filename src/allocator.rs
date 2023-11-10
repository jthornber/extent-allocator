use crate::tree::*;

use roaring::RoaringBitmap;
use std::sync::{Arc, Mutex};

#[cfg(test)]
mod tests;

//----------------------------------------------------------------

pub struct Allocator {
    pub nr_blocks: u64,
    allocated: Mutex<RoaringBitmap>,
    pub extents: Tree,
}

impl Allocator {
    pub fn new(nr_blocks: u64, nr_nodes: u8) -> Self {
        // Create a tree that brackets the entire address space
        let extents = Tree::new(nr_blocks, nr_nodes);

        Allocator {
            nr_blocks,
            allocated: Mutex::new(RoaringBitmap::new()),
            extents,
        }
    }

    pub fn preallocate_random(&mut self, count: u64) {
        let mut allocated = self.allocated.lock().unwrap();
        for _ in 0..count {
            loop {
                let block = rand::random::<u64>() % self.nr_blocks;
                if !allocated.contains(block as u32) {
                    allocated.insert(block as u32);
                    break;
                }
            }
        }
    }

    // FIXME: try with an offset
    pub fn preallocate_linear(&mut self, count: u64, offset: u64) {
        assert!(offset + count <= self.nr_blocks);
        let mut allocated = self.allocated.lock().unwrap();
        for block in 0..count {
            allocated.insert((offset + block) as u32);
        }
    }

    pub fn get_extent(&mut self) -> Option<Arc<Mutex<Extent>>> {
        self.extents.borrow()
    }

    pub fn put_extent(&mut self, extent: Arc<Mutex<Extent>>) {
        self.extents.release(extent);
    }

    pub fn alloc(&mut self, extent: Arc<Mutex<Extent>>) -> Option<u64> {
        let mut extent = extent.lock().unwrap();
        let mut allocated = self.allocated.lock().unwrap();

        for block in extent.cursor..extent.end {
            if allocated.contains(block as u32) {
                continue;
            }

            allocated.insert(block as u32);
            extent.cursor += 1;
            return Some(block);
        }

        extent.cursor = extent.end;
        None
    }
}

//----------------------------------------------------------------

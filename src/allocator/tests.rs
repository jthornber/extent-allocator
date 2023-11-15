use anyhow::Result;
use roaring::RoaringBitmap;
use std::sync::{Arc, Mutex};

use crate::allocator::*;
use crate::tree::utils::*;

//----------------------------------------------------------------

struct AllocationContext {
    inner: Arc<Mutex<AllocContext>>,
    blocks: Vec<u64>,
}

impl AllocationContext {
    fn new(inner: Arc<Mutex<AllocContext>>) -> Self {
        Self {
            inner,
            blocks: Vec::new(),
        }
    }

    fn alloc<F>(&mut self, allocator: &mut Allocator, f: F) -> io::Result<Option<u64>>
    where
        F: FnMut(u64, u64) -> io::Result<Option<u64>>,
    {
        match allocator.alloc(self.inner.clone(), f) {
            Ok(Some(block)) => {
                self.blocks.push(block);
                Ok(Some(block))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

//----------------------------------------------------------------

fn _preallocate_random(allocated: &mut RoaringBitmap, count: u64, range: std::ops::Range<u64>) {
    let len = range.end - range.start;
    for _ in 0..count {
        loop {
            let block = rand::random::<u64>() % len + range.start;
            if !allocated.contains(block as u32) {
                allocated.insert(block as u32);
                break;
            }
        }
    }
}

fn preallocate_linear(allocated: &mut RoaringBitmap, count: u64, offset: u64) {
    allocated.insert_range((offset as u32)..(offset + count) as u32);
}

fn alloc_block(allocated: &mut RoaringBitmap, begin: u64, end: u64) -> io::Result<Option<u64>> {
    for block in begin..end {
        if !allocated.contains(block as u32) {
            allocated.insert(block as u32);
            return Ok(Some(block));
        }
    }
    Ok(None)
}

//----------------------------------------------------------------

#[test]
fn test_prealloc_linear() -> Result<()> {
    // Check we can handle a non-power-of-two number of blocks
    let nr_blocks = 1024;
    let nr_nodes = 255;
    let nr_contexts = 16;
    let allocated = Mutex::new(RoaringBitmap::new());

    preallocate_linear(&mut allocated.lock().unwrap(), nr_blocks / 5, 100);

    let mut allocator = Allocator::new(nr_blocks, nr_nodes);
    let mut contexts = Vec::new();
    for _i in 0..nr_contexts {
        contexts.push(AllocationContext::new(allocator.get_context()));
    }

    for i in 0..(nr_blocks / 2) {
        let context = &mut contexts[(i % nr_contexts) as usize];
        context.alloc(&mut allocator, |begin, end| {
            let mut allocated = allocated.lock().unwrap();
            alloc_block(&mut allocated, begin, end)
        })?;
    }

    //   dump_tree(&allocator.extents);
    //   draw_tree(&allocator.extents);

    for context in contexts {
        allocator.put_context(context.inner);
        print_blocks(&context.blocks);
    }

    dump_tree(&allocator.extents);
    draw_tree(&allocator.extents);

    Ok(())
}

//----------------------------------------------------------------

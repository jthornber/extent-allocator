use anyhow::Result;
use std::sync::{Arc, Mutex};

use crate::allocator::Allocator;
use crate::tree::utils::*;
use crate::tree::Extent;

//----------------------------------------------------------------

pub struct AllocContext {
    extent: Option<Arc<Mutex<Extent>>>,
    blocks: Vec<u64>,
}

#[test]
fn test_prealloc_linear() -> Result<()> {
    // Check we can handle a non-power-of-two number of blocks
    let nr_blocks = 1024;
    let nr_nodes = 255;
    let nr_allocators = 16;

    let mut allocator = Allocator::new(nr_blocks, nr_nodes);
    allocator.preallocate_linear(nr_blocks / 5, 100);

    let mut contexts = Vec::new();
    for _i in 0..nr_allocators {
        contexts.push(AllocContext {
            extent: allocator.get_extent(),
            blocks: Vec::new(),
        });
    }

    for i in 0..(nr_blocks / 2) {
        let context = &mut contexts[(i % nr_allocators) as usize];
        loop {
            let extent = context.extent.as_ref().unwrap().clone();
            let block = allocator.alloc(extent.clone());
            if let Some(block) = block {
                context.blocks.push(block);
                break;
            }

            allocator.put_extent(extent.clone());
            context.extent = allocator.get_extent();
        }
    }

    //   dump_tree(&allocator.extents);
    //   draw_tree(&allocator.extents);

    for context in &mut contexts {
        let extent = context.extent.take();

        allocator.put_extent(extent.unwrap());
        print_blocks(&context.blocks);
    }

    dump_tree(&allocator.extents);
    draw_tree(&allocator.extents);

    Ok(())
}

//----------------------------------------------------------------

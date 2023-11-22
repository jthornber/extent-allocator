use anyhow::{ensure, Result};
use roaring::RoaringBitmap;
use std::sync::{Arc, Mutex};

use crate::allocator::*;
use crate::tree::utils::*;

//----------------------------------------------------------------

struct AllocationContext {
    inner: Option<Arc<Mutex<AllocContext>>>,
    blocks: Vec<u64>,
}

impl AllocationContext {
    fn new(inner: Arc<Mutex<AllocContext>>) -> Self {
        Self {
            inner: Some(inner),
            blocks: Vec::new(),
        }
    }

    fn alloc<F>(&mut self, allocator: &mut Allocator, f: F) -> io::Result<Option<u64>>
    where
        F: FnMut(u64, u64) -> io::Result<Option<u64>>,
    {
        match allocator.alloc(self.inner.as_ref().unwrap().clone(), f) {
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

fn preallocate_random(allocated: &mut RoaringBitmap, count: u64, range: std::ops::Range<u64>) {
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

fn context_alloc(
    context: &mut AllocationContext,
    allocator: &mut Allocator,
    allocated: &Arc<Mutex<RoaringBitmap>>,
) -> io::Result<Option<u64>> {
    context.alloc(allocator, |begin, end| {
        let mut allocated = allocated.lock().unwrap();
        alloc_block(&mut allocated, begin, end)
    })
}

fn do_allocation_test(
    nr_blocks: u64,
    nr_contexts: usize,
    allocated: Arc<Mutex<RoaringBitmap>>,
    nr_blocks_to_allocate: u64,
) -> Result<Vec<AllocationContext>> {
    let nr_nodes = 255;
    let mut allocator = Allocator::new(nr_blocks, nr_nodes);

    let mut contexts = Vec::new();
    for _i in 0..nr_contexts {
        contexts.push(AllocationContext::new(allocator.get_context()));
    }

    let nr_prealloc = allocated.lock().unwrap().len();

    for i in 0..nr_blocks_to_allocate {
        let context = &mut contexts[(i % nr_contexts as u64) as usize];
        context_alloc(context, &mut allocator, &allocated)?;
    }

    //   dump_tree(&allocator.extents);
    //   draw_tree(&allocator.extents);

    let mut total_nr_allocated = 0;
    for (i, context) in contexts.iter_mut().enumerate() {
        allocator.put_context(context.inner.take().unwrap());
        total_nr_allocated += context.blocks.len() as u64;

        // verify the number of blocks allocated per context
        let mut expected = nr_blocks_to_allocate / nr_contexts as u64;
        if (i as u64) < nr_blocks_to_allocate % (nr_contexts as u64) {
            expected += 1;
        }
        ensure!(context.blocks.len() as u64 == expected);
    }

    let nr_allocated = allocated.lock().unwrap().len();
    ensure!(total_nr_allocated == nr_blocks_to_allocate);
    ensure!(nr_allocated - nr_prealloc == nr_blocks_to_allocate);

    dump_tree(&allocator.extents);
    draw_tree(&allocator.extents);

    Ok(contexts)
}

fn do_reset_test(nr_contexts: usize) -> Result<()> {
    let nr_blocks = 1024;
    let mut allocator = Allocator::new(nr_blocks, 1);

    let allocated = Arc::new(Mutex::new(RoaringBitmap::new()));
    preallocate_linear(
        &mut allocated.lock().unwrap(),
        nr_blocks - nr_contexts as u64,
        0,
    );

    let mut contexts = Vec::new();
    for _i in 0..nr_contexts {
        contexts.push(AllocationContext::new(allocator.get_context()));
    }

    for context in &mut contexts {
        ensure!(matches!(
            context_alloc(context, &mut allocator, &allocated),
            Ok(Some(_))
        ));
    }

    for context in &mut contexts {
        let ctx = context.inner.as_ref().unwrap();
        ensure!(ctx.lock().unwrap().extent.is_none());
    }

    Ok(())
}

fn do_remove_holders_test(reorder: &dyn Fn(&mut Vec<AllocationContext>)) -> Result<()> {
    let nr_blocks = 1024;
    let mut allocator = Allocator::new(nr_blocks, 1);

    let allocated = Arc::new(Mutex::new(RoaringBitmap::new()));

    let mut contexts = Vec::new();
    let nr_contexts = 3;
    for _i in 0..nr_contexts {
        contexts.push(AllocationContext::new(allocator.get_context()));
    }

    for context in &mut contexts {
        ensure!(matches!(
            context_alloc(context, &mut allocator, &allocated),
            Ok(Some(_))
        ));
    }

    reorder(&mut contexts);

    for mut context in contexts {
        allocator.put_context(context.inner.take().unwrap());
    }

    ensure!(allocator.holders.is_empty());

    Ok(())
}

//----------------------------------------------------------------

// TODO: Check we can handle a non-power-of-two number of blocks

#[test]
fn test_prealloc_linear_start() -> Result<()> {
    let nr_blocks = 1024;
    let nr_contexts = 16;
    let allocated = Arc::new(Mutex::new(RoaringBitmap::new()));
    let nr_prealloc = nr_blocks / 5;

    preallocate_linear(&mut allocated.lock().unwrap(), nr_prealloc, 0);

    let contexts = do_allocation_test(nr_blocks, nr_contexts, allocated, nr_blocks / 2)?;

    for context in contexts {
        print_blocks(&context.blocks);
    }

    Ok(())
}

#[test]
fn test_prealloc_linear_middle() -> Result<()> {
    let nr_blocks = 1024;
    let nr_contexts = 16;
    let allocated = Arc::new(Mutex::new(RoaringBitmap::new()));
    let nr_prealloc = nr_blocks / 5;

    preallocate_linear(&mut allocated.lock().unwrap(), nr_prealloc, 100);

    let contexts = do_allocation_test(nr_blocks, nr_contexts, allocated, nr_blocks / 2)?;

    for context in contexts {
        print_blocks(&context.blocks);
    }

    Ok(())
}

#[test]
fn test_prealloc_linear_end() -> Result<()> {
    let nr_blocks = 1024;
    let nr_contexts = 16;
    let allocated = Arc::new(Mutex::new(RoaringBitmap::new()));
    let nr_prealloc = nr_blocks / 5;

    preallocate_linear(
        &mut allocated.lock().unwrap(),
        nr_prealloc,
        nr_blocks - nr_prealloc,
    );

    let contexts = do_allocation_test(nr_blocks, nr_contexts, allocated, nr_blocks / 2)?;

    for context in contexts {
        print_blocks(&context.blocks);
    }

    Ok(())
}

#[test]
fn test_prealloc_random() -> Result<()> {
    let nr_blocks = 1024;
    let nr_contexts = 16;
    let allocated = Arc::new(Mutex::new(RoaringBitmap::new()));
    let nr_prealloc = nr_blocks / 5;

    preallocate_random(&mut allocated.lock().unwrap(), nr_prealloc, 0..nr_blocks);

    let contexts = do_allocation_test(nr_blocks, nr_contexts, allocated, nr_blocks / 2)?;

    for context in contexts {
        print_blocks(&context.blocks);
    }

    Ok(())
}

#[test]
fn test_non_power_of_two_blocks() -> Result<()> {
    let nr_blocks = 1031;
    let nr_contexts = 16;
    let allocated = Arc::new(Mutex::new(RoaringBitmap::new()));

    let contexts = do_allocation_test(nr_blocks, nr_contexts, allocated, nr_blocks)?;

    for context in contexts {
        print_blocks(&context.blocks);
    }

    Ok(())
}

#[test]
fn alloc_no_space() -> Result<()> {
    let allocated = Arc::new(Mutex::new(RoaringBitmap::new()));

    let nr_blocks = 1024;
    let nr_nodes = 1;
    let mut allocator = Allocator::new(nr_blocks, nr_nodes);
    let mut context = AllocationContext::new(allocator.get_context());

    while let Ok(Some(_)) = context_alloc(&mut context, &mut allocator, &allocated) {}

    ensure!(matches!(
        context_alloc(&mut context, &mut allocator, &allocated),
        Ok(None)
    ));

    Ok(())
}

#[test]
fn alloc_after_reset() -> Result<()> {
    let nr_blocks = 1024;
    let allocated = Arc::new(Mutex::new(RoaringBitmap::new()));
    let nr_prealloc = nr_blocks / 5;

    preallocate_linear(&mut allocated.lock().unwrap(), nr_prealloc, 0);

    let nr_nodes = 1;
    let mut allocator = Allocator::new(nr_blocks, nr_nodes);
    let mut context = AllocationContext::new(allocator.get_context());

    while let Ok(Some(_)) = context_alloc(&mut context, &mut allocator, &allocated) {}

    ensure!(context.blocks.len() as u64 == nr_blocks - nr_prealloc);

    allocated
        .lock()
        .unwrap()
        .remove_range(0..(nr_prealloc as u32));
    allocator.reset();

    while let Ok(Some(_)) = context_alloc(&mut context, &mut allocator, &allocated) {}

    ensure!(context.blocks.len() as u64 == nr_blocks);

    Ok(())
}

#[test]
fn alloc_after_resize() -> Result<()> {
    let nr_blocks = 1024;
    let allocated = Arc::new(Mutex::new(RoaringBitmap::new()));

    let nr_nodes = 1;
    let mut allocator = Allocator::new(nr_blocks, nr_nodes);
    let mut context = AllocationContext::new(allocator.get_context());

    while let Ok(Some(_)) = context_alloc(&mut context, &mut allocator, &allocated) {}

    ensure!(context.blocks.len() as u64 == nr_blocks);

    let nr_blocks = 2048;
    allocator.resize(nr_blocks);

    while let Ok(Some(_)) = context_alloc(&mut context, &mut allocator, &allocated) {}

    ensure!(context.blocks.len() as u64 == nr_blocks);

    Ok(())
}

#[test]
fn reset_two_holders() -> Result<()> {
    do_reset_test(2)
}

#[test]
fn reset_three_holders() -> Result<()> {
    do_reset_test(3)
}

#[test]
fn remove_holders_backward() -> Result<()> {
    // remove contexts, starting from the tail of the holders list
    do_remove_holders_test(&|_| {})
}

#[test]
fn remove_holders_forward() -> Result<()> {
    // remove contexts, starting from the head of the holders list
    do_remove_holders_test(&|contexts| {
        contexts.swap(0, 2);
    })
}

#[test]
fn remove_holders_from_middle() -> Result<()> {
    do_remove_holders_test(&|contexts| {
        let c = contexts.remove(0);
        contexts.push(c);
    })
}

//----------------------------------------------------------------

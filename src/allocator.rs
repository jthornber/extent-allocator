use crate::tree::*;

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex, Weak};

#[cfg(test)]
mod tests;

//----------------------------------------------------------------

pub struct AllocContext {
    extent: Option<Arc<Mutex<Extent>>>,
    next: Option<Arc<Mutex<Self>>>,
    prev: Option<Weak<Mutex<Self>>>,
}

impl AllocContext {
    fn new() -> Self {
        Self {
            extent: None,
            prev: None,
            next: None,
        }
    }
}

fn reset_chained_contexts(ac: &mut AllocContext) {
    ac.extent = None;
    ac.prev = None;
    let mut next = ac.next.take();

    while let Some(n) = next {
        let mut ac = n.lock().unwrap();
        ac.extent = None;
        ac.prev = None;
        next = ac.next.take();
    }
}

pub struct Allocator {
    extents: Tree,
    holders: BTreeMap<u64, Arc<Mutex<AllocContext>>>,
}

impl Allocator {
    pub fn new(nr_blocks: u64, nr_nodes: u8) -> Self {
        // Create a tree that brackets the entire address space
        let extents = Tree::new(nr_blocks, nr_nodes);

        Allocator {
            extents,
            holders: BTreeMap::new(),
        }
    }

    pub fn get_context(&mut self) -> Arc<Mutex<AllocContext>> {
        Arc::new(Mutex::new(AllocContext::new()))
    }

    pub fn put_context(&mut self, context: Arc<Mutex<AllocContext>>) {
        let mut ctx = context.lock().unwrap();

        if let Some(extent) = ctx.extent.take() {
            let extent_begin = extent.lock().unwrap().begin;
            self.remove_holder(extent_begin, &mut ctx);
            self.extents.release(extent);
        }
    }

    pub fn alloc<F>(
        &mut self,
        context: Arc<Mutex<AllocContext>>,
        mut f: F,
    ) -> io::Result<Option<u64>>
    where
        F: FnMut(u64, u64) -> io::Result<Option<u64>>,
    {
        loop {
            let mut ctx = context.lock().unwrap();

            if ctx.extent.is_none() {
                ctx.extent = self.extents.borrow();

                #[allow(clippy::question_mark)]
                if ctx.extent.is_none() {
                    return Ok(None); // -ENOSPC
                }

                let extent_begin = ctx.extent.as_ref().unwrap().lock().unwrap().begin;
                self.add_holder(extent_begin, &context, &mut ctx);
            }

            let mut extent = ctx.extent.as_ref().unwrap().lock().unwrap();

            match f(extent.cursor, extent.end) {
                Ok(Some(b)) => {
                    extent.cursor = b + 1;
                    if extent.cursor == extent.end {
                        drop(extent);
                        drop(ctx);
                        self.reset_and_release(context.clone());
                    }
                    return Ok(Some(b));
                }
                Ok(None) => {
                    extent.cursor = extent.end;
                    drop(extent);
                    drop(ctx);
                    self.reset_and_release(context.clone());
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn add_holder(
        &mut self,
        extent_begin: u64,
        context: &Arc<Mutex<AllocContext>>,
        ctx: &mut AllocContext,
    ) {
        self.holders
            .entry(extent_begin)
            .and_modify(|head| {
                ctx.next = Some(head.clone());
                head.lock().unwrap().prev = Some(Arc::<Mutex<AllocContext>>::downgrade(context));
                std::mem::swap(&mut context.clone(), head);
            })
            .or_insert(context.clone());
    }

    fn remove_holder(&mut self, extent_begin: u64, ctx: &mut AllocContext) {
        match (ctx.prev.take(), ctx.next.take()) {
            (None, None) => {
                self.holders.remove(&extent_begin);
            }
            (None, Some(mut next)) => {
                self.holders.entry(extent_begin).and_modify(|head| {
                    next.lock().unwrap().prev = None;
                    std::mem::swap(&mut next, head);
                });
            }
            (Some(prev), next) => {
                if let Some(p) = prev.upgrade() {
                    p.lock().unwrap().next = next.clone();
                }
                if let Some(next) = next {
                    next.lock().unwrap().prev = Some(prev);
                }
            }
        }
    }

    fn reset_and_release(&mut self, context: Arc<Mutex<AllocContext>>) {
        let ctx = context.lock().unwrap();
        let old_extent = ctx.extent.clone();
        drop(ctx);

        let extent_begin = old_extent.as_ref().unwrap().lock().unwrap().begin;
        self.reset_contexts(extent_begin);
        self.extents.release(old_extent.unwrap());
    }

    fn reset_contexts(&mut self, extent_begin: u64) {
        if let Some(holders) = self.holders.remove(&extent_begin) {
            let mut ac = holders.lock().unwrap();
            reset_chained_contexts(&mut ac);
        }
    }

    fn reset_all_contexts(&mut self) {
        let mut holders = BTreeMap::new();
        std::mem::swap(&mut holders, &mut self.holders);

        for (_, holders) in holders {
            let mut ac = holders.lock().unwrap();
            reset_chained_contexts(&mut ac);
        }
    }

    pub fn reset(&mut self) {
        self.reset_all_contexts();
        self.extents.reset();
    }

    pub fn resize(&mut self, nr_blocks: u64) {
        self.reset_all_contexts();
        self.extents.resize(nr_blocks);
    }
}

//----------------------------------------------------------------

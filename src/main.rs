use anyhow::Result;
use roaring::RoaringBitmap;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use rand::*;

//----------------------------------------------------------------

struct Internal {
    begin: u64,
    end: u64,
    contains_free_blocks: bool,
    holders: usize,
    left: Box<Node>,
    right: Box<Node>,
}

#[derive(Clone, Copy, Debug)]
struct Extent {
    begin: u64,
    end: u64,
    cursor: u64,
}

struct Leaf {
    extent: Arc<Mutex<Extent>>,
    contains_free_blocks: bool,
    holders: usize,
}

enum Node {
    Internal(Internal),
    Leaf(Leaf),
}

struct Tree {
    root: Node,
}

fn build_tree(begin: u64, end: u64, levels: usize) -> Node {
    if levels == 0 {
        return Node::Leaf(Leaf {
            extent: Arc::new(Mutex::new(Extent {
                begin,
                end,
                cursor: begin,
            })),
            contains_free_blocks: true,
            holders: 0,
        });
    }

    let mid = begin + (end - begin) / 2;

    let left = Box::new(build_tree(begin, mid, levels - 1));
    let right = Box::new(build_tree(mid, end, levels - 1));

    Node::Internal(Internal {
        begin,
        end,
        contains_free_blocks: true,
        holders: 0,
        left,
        right,
    })
}

fn dump_tree(node: &Node, indent: usize) {
    // Create a string of spaces for indentation
    let pad = (0..indent).map(|_| ' ').collect::<String>();

    match node {
        Node::Internal(node) => {
            println!(
                "{}Internal: b={} e={} free={} holders={}",
                pad, node.begin, node.end, node.contains_free_blocks, node.holders,
            );
            dump_tree(&node.left, indent + 2);
            dump_tree(&node.right, indent + 2);
        }

        Node::Leaf(node) => {
            let extent = node.extent.lock().unwrap();
            println!(
                "{}Leaf: b={} e={} cursor={} free={} holders={}",
                pad,
                extent.begin,
                extent.end,
                extent.cursor,
                node.contains_free_blocks,
                node.holders,
            );
        }
    }
}

impl Node {
    fn nr_holders(&self) -> usize {
        match self {
            Node::Internal(node) => node.holders,
            Node::Leaf(node) => node.holders,
        }
    }

    fn contains_free_blocks(&self) -> bool {
        match self {
            Node::Internal(node) => node.contains_free_blocks,
            Node::Leaf(node) => node.contains_free_blocks,
        }
    }

    fn contains(&self, b: u64) -> bool {
        match self {
            Node::Internal(node) => b >= node.begin && b < node.end,
            Node::Leaf(node) => {
                let extent = node.extent.lock().unwrap();
                b >= extent.begin && b < extent.end
            }
        }
    }
}

impl Tree {
    fn new(nr_blocks: u64, nr_levels: usize) -> Self {
        Tree {
            root: build_tree(0, nr_blocks, nr_levels),
        }
    }

    fn borrow_(node: &mut Node) -> Option<Arc<Mutex<Extent>>> {
        match node {
            Node::Internal(node) => {
                if !node.contains_free_blocks {
                    return None;
                }

                let extent = match (
                    node.left.contains_free_blocks(),
                    node.right.contains_free_blocks(),
                ) {
                    (false, false) => None,
                    (true, false) => Self::borrow_(&mut node.left),
                    (false, true) => Self::borrow_(&mut node.right),
                    (true, true) => {
                        // Both children have free blocks, so select the one with the fewest holders
                        let nr_left_holders = node.left.nr_holders();
                        let nr_right_holders = node.right.nr_holders();

                        if nr_left_holders <= nr_right_holders {
                            Self::borrow_(&mut node.left)
                        } else {
                            Self::borrow_(&mut node.right)
                        }
                    }
                };

                if extent.is_some() {
                    node.holders += 1;
                }
                return extent;
            }

            Node::Leaf(node) => {
                if node.contains_free_blocks {
                    node.holders += 1;
                    return Some(node.extent.clone());
                }

                return None;
            }
        }
    }

    // Returns a region that has some free blocks.  This can
    // cause existing regions to be altered as new splits are
    // introduced to the BSP tree.
    fn borrow(&mut self) -> Option<Arc<Mutex<Extent>>> {
        Self::borrow_(&mut self.root)
    }

    // Returns a bool indicating whether the extent contains free blocks
    fn release_(node: &mut Node, b: u64) {
        match node {
            Node::Internal(node) => {
                assert!(node.holders > 0);
                node.holders -= 1;

                // FIXME: refactor
                if node.left.contains(b) {
                    // The extent is in the left subtree
                    Self::release_(&mut node.left, b);
                } else {
                    // The extent is in the right subtree
                    Self::release_(&mut node.right, b);
                }

                let left_free = node.left.contains_free_blocks();
                let right_free = node.right.contains_free_blocks();
                node.contains_free_blocks = left_free || right_free;
            }

            Node::Leaf(node) => {
                assert!(node.holders > 0);
                node.holders -= 1;

                // See if the extent is now empty
                let extent = node.extent.lock().unwrap();
                if extent.cursor == extent.end {
                    node.contains_free_blocks = false;
                }
            }
        }
    }

    fn release(&mut self, extent: Arc<Mutex<Extent>>) {
        // eprintln!("before release:");
        // dump_tree(&self.root, 0);

        let extent = extent.lock().unwrap();
        let b = extent.begin;
        drop(extent);

        Self::release_(&mut self.root, b);

        // eprintln!("after release:");
        // dump_tree(&self.root, 0);
    }

    // Resets the cursor to the beginning of the extent and propagates
    // the contains_free_blocks flag up the tree.
    fn mark_free_(node: &mut Node, block: u64) {
        match node {
            Node::Internal(node) => {
                if block < node.begin || block >= node.end {
                    return;
                }

                Self::mark_free_(&mut node.left, block);
                Self::mark_free_(&mut node.right, block);

                node.contains_free_blocks = true;
            }

            Node::Leaf(node) => {
                let mut extent = node.extent.lock().unwrap();

                if block < extent.begin || block >= extent.end {
                    return;
                }

                extent.cursor = extent.begin;
                node.contains_free_blocks = true;
            }
        }
    }

    fn mark_free(&mut self, block: u64) {
        Self::mark_free_(&mut self.root, block);
    }
}

//----------------------------------------------------------------

struct Allocator {
    nr_blocks: u64,
    allocated: Mutex<RoaringBitmap>,
    extents: Tree,
}

#[derive(Clone)]
struct Cursor {
    extent: Arc<Mutex<Extent>>,
}

impl Allocator {
    fn new(nr_blocks: u64, nr_levels: usize) -> Self {
        // Create a tree that brackets the entire address space
        let extents = Tree::new(nr_blocks, nr_levels);

        Allocator {
            nr_blocks,
            allocated: Mutex::new(RoaringBitmap::new()),
            extents,
        }
    }

    fn preallocate(&mut self, count: u64) {
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

    fn log_extent(msg: &str, extent: &Option<Arc<Mutex<Extent>>>) {
        let extent = match extent {
            Some(extent) => extent.lock().unwrap(),
            None => return,
        };
        println!("{}: extent: {:?}", msg, extent);
    }

    fn get_extent(&mut self) -> Option<Arc<Mutex<Extent>>> {
        let e = self.extents.borrow();
        //Self::log_extent("get_extent", &e);
        return e;
    }

    fn put_extent(&mut self, extent: Arc<Mutex<Extent>>) {
        // Self::log_extent("before put_extent", &Some(extent.clone()));
        self.extents.release(extent);
    }

    fn alloc(&mut self, extent: Arc<Mutex<Extent>>) -> Option<u64> {
        let mut extent = extent.lock().unwrap();
        let mut allocated = self.allocated.lock().unwrap();

        for block in extent.cursor..extent.end {
            if allocated.contains(block as u32) {
                continue;
            }

            allocated.insert(block as u32);
            extent.cursor = extent.cursor + 1;
            return Some(block as u64);
        }

        extent.cursor = extent.end;
        None
    }

    // We do not free blocks through an extent.
    fn free(&mut self, block: u64) {
        let mut allocated = self.allocated.lock().unwrap();
        allocated.remove(block as u32);
        self.extents.mark_free(block);
    }
}

//----------------------------------------------------------------

struct AllocContext {
    extent: Option<Arc<Mutex<Extent>>>,
    blocks: Vec<u64>,
}

fn to_runs(blocks: &[u64]) -> Vec<(u64, u64)> {
    let mut runs = Vec::new();
    let mut begin = blocks[0];
    let mut end = begin;
    for &block in blocks.iter().skip(1) {
        if block == end + 1 {
            end = block;
        } else {
            runs.push((begin, end));
            begin = block;
            end = block;
        }
    }
    runs.push((begin, end));
    runs
}

fn print_blocks(blocks: &[u64]) {
    let runs = to_runs(blocks);
    let mut first = true;
    print!("[");
    for (begin, end) in runs {
        if first {
            first = false;
        } else {
            print!(", ");
        }
        if begin == end {
            print!("{}", begin);
        } else {
            print!("{}..{}", begin, end);
        }
    }
    println!("]");
}

fn main() {
    // Check we can handle a non-power-of-two number of blocks
    let nr_blocks = 1024;
    let nr_levels = 3;
    let nr_allocators = 8;

    let mut allocator = Allocator::new(nr_blocks, nr_levels);
    //allocator.preallocate(nr_blocks / 4);

    let mut contexts = Vec::new();
    for i in 0..nr_allocators {
        contexts.push(AllocContext {
            extent: allocator.get_extent(),
            blocks: Vec::new(),
        });
    }

    for i in 0..(nr_blocks / 2) {
        let mut context = &mut contexts[(i % nr_allocators) as usize];
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

    for context in &mut contexts {
        let extent = context.extent.take();

        allocator.put_extent(extent.unwrap());
        print_blocks(&context.blocks);
    }
}

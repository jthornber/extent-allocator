use std::sync::{Arc, Mutex};

pub mod utils;

#[cfg(test)]
mod tests;

//----------------------------------------------------------------

pub const NULL_NODE: u8 = 255;

//----------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct Extent {
    pub begin: u64,
    pub end: u64,
    pub cursor: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct Internal {
    holders: usize,
    nr_free_blocks: u64,
    cut: u64,
    left: u8,
    right: u8,
}

#[derive(Clone, Debug)]
pub struct Leaf {
    extent: Arc<Mutex<Extent>>,
    holders: usize,
}

//----------------------------------------------------------------

#[derive(Clone, Debug)]
pub enum Node {
    Internal(Internal),
    Leaf(Leaf),
}

impl Node {
    pub fn nr_holders(&self) -> usize {
        match self {
            Node::Internal(node) => node.holders,
            Node::Leaf(node) => node.holders,
        }
    }

    pub fn nr_free_blocks(&self) -> u64 {
        match self {
            Node::Internal(node) => node.nr_free_blocks,
            Node::Leaf(node) => {
                let extent = node.extent.lock().unwrap();
                extent.end - extent.cursor
            }
        }
    }
}

impl Default for Node {
    fn default() -> Self {
        Node::Internal(Internal {
            holders: 0,
            nr_free_blocks: 0,
            cut: 0,
            left: NULL_NODE,
            right: NULL_NODE,
        })
    }
}

//----------------------------------------------------------------

pub struct Tree {
    nr_blocks: u64,
    nodes: Vec<Node>,
    free_nodes: Vec<u8>,
    root: u8,
}

impl Tree {
    pub fn new(nr_blocks: u64, nr_nodes: u8) -> Self {
        #[allow(clippy::absurd_extreme_comparisons)]
        {
            assert!(nr_nodes <= NULL_NODE);
        }

        let free_nodes = (0u8..nr_nodes).collect::<Vec<u8>>();
        let mut tree = Tree {
            nr_blocks,
            nodes: vec![Node::default(); nr_nodes as usize],
            free_nodes,
            root: NULL_NODE,
        };

        tree.root = tree.alloc_node().unwrap();
        tree.nodes[tree.root as usize] = Node::Leaf(Leaf {
            extent: Arc::new(Mutex::new(Extent {
                begin: 0,
                end: nr_blocks,
                cursor: 0,
            })),
            holders: 0,
        });

        tree
    }

    fn alloc_node(&mut self) -> Option<u8> {
        self.free_nodes.pop()
    }

    fn free_node(&mut self, node: u8) {
        self.free_nodes.push(node);
    }

    pub fn read_node(&self, node: u8) -> Node {
        self.nodes[node as usize].clone()
    }

    fn write_node(&mut self, node: u8, node_data: Node) {
        self.nodes[node as usize] = node_data;
    }

    fn split_leaf(&mut self, node_index: u8) -> bool {
        if self.free_nodes.len() < 2 {
            return false;
        }

        let node = self.read_node(node_index);
        match node {
            Node::Internal(_) => panic!("split_leaf called on internal node"),
            Node::Leaf(leaf) => {
                // We copy the extent, because we're about to adjust it and
                // reuse for one of the children.
                let mut extent = leaf.extent.lock().unwrap();

                if extent.end - extent.cursor <= 16 {
                    // We can't split this leaf, because it's too small
                    return false;
                }

                let copy = *extent;
                let mid = extent.cursor + (extent.end - extent.cursor) / 2;
                extent.end = mid;
                drop(extent);

                let left_child = self.alloc_node().unwrap();
                let right_child = self.alloc_node().unwrap();

                self.write_node(
                    left_child,
                    Node::Leaf(Leaf {
                        extent: leaf.extent,
                        holders: leaf.holders,
                    }),
                );
                self.write_node(
                    right_child,
                    Node::Leaf(Leaf {
                        extent: Arc::new(Mutex::new(Extent {
                            begin: mid,
                            end: copy.end,
                            cursor: mid,
                        })),
                        holders: 0,
                    }),
                );

                // Now turn the old leaf into an internal node
                let nr_holders = leaf.holders;
                self.write_node(
                    node_index,
                    Node::Internal(Internal {
                        cut: mid,
                        holders: nr_holders,
                        nr_free_blocks: copy.end - copy.cursor,
                        left: left_child,
                        right: right_child,
                    }),
                );
            }
        }
        true
    }

    // Select a child to borrow, based on the nr of holders and the nr of free blocks
    fn select_child(&self, left: u8, right: u8) -> u8 {
        assert!(left != NULL_NODE);
        assert!(right != NULL_NODE);

        let left_node = self.read_node(left);
        let right_node = self.read_node(right);

        let left_holders = left_node.nr_holders();
        let left_free = left_node.nr_free_blocks();
        let left_score = left_free / (left_holders + 1) as u64;

        let right_holders = right_node.nr_holders();
        let right_free = right_node.nr_free_blocks();
        let right_score = right_free / (right_holders + 1) as u64;

        if left_score >= right_score {
            left
        } else {
            right
        }
    }

    fn borrow_(&mut self, node_index: u8) -> Option<Arc<Mutex<Extent>>> {
        if node_index == NULL_NODE {
            return None;
        }

        let node = self.read_node(node_index);
        match node {
            Node::Internal(node) => {
                let extent = match (node.left, node.right) {
                    (255, 255) => panic!("node with two NULLs shouldn't be possible"),
                    (255, right) => self.borrow_(right),
                    (left, 255) => self.borrow_(left),
                    (left, right) => self.borrow_(self.select_child(left, right)),
                };

                if extent.is_some() {
                    self.write_node(
                        node_index,
                        Node::Internal(Internal {
                            cut: node.cut,
                            holders: node.holders + 1,
                            nr_free_blocks: node.nr_free_blocks,
                            left: node.left,
                            right: node.right,
                        }),
                    );
                }
                extent
            }

            Node::Leaf(node) => {
                if node.holders > 0 {
                    // Someone is already using this extent.  See if we can split it.
                    if self.split_leaf(node_index) {
                        // Try again, now that this node is an internal node
                        self.borrow_(node_index)
                    } else {
                        // We can't split the leaf, so we'll have to share.
                        self.write_node(
                            node_index,
                            Node::Leaf(Leaf {
                                extent: node.extent.clone(),
                                holders: node.holders + 1,
                            }),
                        );
                        Some(node.extent)
                    }
                } else {
                    // No one is using this extent, so we can just take it.
                    self.write_node(
                        node_index,
                        Node::Leaf(Leaf {
                            extent: node.extent.clone(),
                            holders: node.holders + 1,
                        }),
                    );
                    Some(node.extent)
                }
            }
        }
    }

    // Returns a region that has some free blocks.  This can
    // cause existing regions to be altered as new splits are
    // introduced to the BSP tree.
    pub fn borrow(&mut self) -> Option<Arc<Mutex<Extent>>> {
        self.borrow_(self.root)
    }

    fn nr_free(&self, node_index: u8) -> u64 {
        if node_index == NULL_NODE {
            return 0;
        }

        let node = self.read_node(node_index);
        node.nr_free_blocks()
    }

    // Returns the node_index of the replacement for this node (commonly the same as node_index)
    #[allow(clippy::only_used_in_recursion)]
    fn release_(&mut self, block: u64, begin: u64, end: u64, node_index: u8) -> u8 {
        if node_index == NULL_NODE {
            return node_index;
        }

        let node = self.read_node(node_index);

        match node {
            Node::Internal(node) => {
                assert!(node.holders > 0);
                let mut left = node.left;
                let mut right = node.right;

                // FIXME: refactor
                if block < node.cut {
                    left = self.release_(block, begin, node.cut, node.left);
                } else {
                    right = self.release_(block, node.cut, end, node.right);
                }

                if left == NULL_NODE && right == NULL_NODE {
                    // Both children are NULL, so we can free this node
                    self.free_node(node_index);
                    NULL_NODE
                } else if left == NULL_NODE {
                    self.free_node(node_index);
                    right
                } else if right == NULL_NODE {
                    self.free_node(node_index);
                    left
                } else {
                    self.write_node(
                        node_index,
                        Node::Internal(Internal {
                            cut: node.cut,
                            holders: node.holders - 1,
                            nr_free_blocks: self.nr_free(left) + self.nr_free(right),
                            left,
                            right,
                        }),
                    );

                    node_index
                }
            }

            Node::Leaf(node) => {
                assert!(node.holders > 0);

                // See if the extent is now empty
                let extent = node.extent.lock().unwrap();
                let full = extent.cursor == extent.end;
                drop(extent);

                if full {
                    // The extent is now empty, so we can free this node
                    self.free_node(node_index);
                    NULL_NODE
                } else {
                    self.write_node(
                        node_index,
                        Node::Leaf(Leaf {
                            extent: node.extent,
                            holders: node.holders - 1,
                        }),
                    );
                    node_index
                }
            }
        }
    }

    pub fn release(&mut self, extent: Arc<Mutex<Extent>>) {
        // eprintln!("before release:");
        // dump_tree(&self.root, 0);

        let extent = extent.lock().unwrap();
        let b = extent.begin;
        drop(extent);

        self.root = self.release_(b, 0, self.nr_blocks, self.root);

        // eprintln!("after release:");
        // dump_tree(&self.root, 0);
    }

    pub fn reset(&mut self) {
        todo!()
    }
}

//----------------------------------------------------------------

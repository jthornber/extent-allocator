use std::collections::VecDeque;

use crate::tree::*;

//----------------------------------------------------------------

pub fn dump_tree(tree: &Tree) {
    let mut stack = vec![(tree.root, 0)];
    while let Some((node_index, indent)) = stack.pop() {
        let pad = (0..indent).map(|_| ' ').collect::<String>();

        if node_index == NULL_NODE {
            println!("{}NULL", pad);
            continue;
        }

        let node = tree.read_node(node_index);
        match node {
            Node::Internal(node) => {
                println!(
                    "{}Internal: cut={} holders={} nr_free={}",
                    pad, node.cut, node.holders, node.nr_free_blocks
                );
                stack.push((node.right, indent + 8));
                stack.push((node.left, indent + 8));
            }

            Node::Leaf(node) => {
                let extent = node.extent.lock().unwrap();
                println!(
                    "{}Leaf: b={} e={} cursor={} holders={}",
                    pad, extent.begin, extent.end, extent.cursor, node.holders,
                );
            }
        }
    }
}

//----------------------------------------------------------------

fn char_run(c: char, fraction: f64, width: usize) -> String {
    let nr_chars = (fraction * width as f64) as usize;
    let mut s = String::new();
    for _ in 0..nr_chars {
        s.push(c);
    }
    s
}

pub fn draw_tree(tree: &Tree) {
    let width = 100;
    let mut deque = VecDeque::new();
    deque.push_back((tree.root, 0, tree.nr_blocks, 0, '-'));

    let mut cursor = 0;
    let mut last_level = None;
    while let Some((node_index, begin, end, level, c)) = deque.pop_front() {
        if node_index == NULL_NODE {
            println!("NULL");
            continue;
        }

        match (last_level, level) {
            (None, level) => {
                cursor = 0;
                last_level = Some(level);
            }
            (Some(last), level) => {
                if last != level {
                    println!();
                    cursor = 0;
                    last_level = Some(level);
                }
            }
        }

        // Print any padding we need.
        if begin > cursor {
            print!(
                "{} ",
                char_run(' ', (begin - cursor) as f64 / tree.nr_blocks as f64, width)
            );
        }

        let node = tree.read_node(node_index);
        match node {
            Node::Internal(node) => {
                // Print the node.
                print!(
                    "{}",
                    char_run(c, (end - begin) as f64 / tree.nr_blocks as f64, width)
                );

                cursor = end;
                if node.left != NULL_NODE {
                    deque.push_back((node.left, begin, node.cut, level + 1, '/'));
                }
                if node.right != NULL_NODE {
                    deque.push_back((node.right, node.cut, end, level + 1, '\\'));
                }
            }

            Node::Leaf(_node) => {
                // Print the node.
                print!(
                    "{}",
                    char_run(c, (end - begin) as f64 / tree.nr_blocks as f64, width)
                );

                cursor = end;
            }
        }
    }
    println!();
}

//----------------------------------------------------------------

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

pub fn print_blocks(blocks: &[u64]) {
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

//----------------------------------------------------------------

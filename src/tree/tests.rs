use anyhow::{ensure, Result};

use crate::tree::utils::*;
use crate::tree::*;

//----------------------------------------------------------------

#[test]
fn borrow_shared_extent() -> Result<()> {
    let nr_blocks = 1024;
    let nr_nodes = 3;

    let mut extents = Vec::new();
    let mut tree = Tree::new(nr_blocks, nr_nodes);
    extents.push(tree.borrow().unwrap());
    extents.push(tree.borrow().unwrap());
    extents.push(tree.borrow().unwrap());
    extents.push(tree.borrow().unwrap());

    check_nr_holders(&tree)?;

    {
        let ext = extents[2].lock().unwrap();
        ensure!(ext.begin == 0);
        ensure!(ext.end == 512);
        ensure!(ext.cursor == 0);
    }

    {
        let ext = extents[3].lock().unwrap();
        ensure!(ext.begin == 512);
        ensure!(ext.end == 1024);
        ensure!(ext.cursor == 512);
    }

    Ok(())
}

// TODO: randomly release multiple leaves
#[test]
fn release_extents_full() -> Result<()> {
    let nr_blocks = 1024;
    let nr_nodes = 3;

    let mut extents = Vec::new();
    let mut tree = Tree::new(nr_blocks, nr_nodes);
    extents.push(tree.borrow().unwrap());
    extents.push(tree.borrow().unwrap());
    ensure!(tree.free_nodes.len() == 0);

    // consume and release the two leaves
    {
        let mut extent = extents[0].lock().unwrap();
        extent.cursor = extent.end;
    }
    tree.release(extents.remove(0));

    {
        let mut extent = extents[0].lock().unwrap();
        extent.cursor = extent.end;
    }
    tree.release(extents.remove(0));

    ensure!(tree.borrow().is_none());
    ensure!(tree.root == NULL_NODE);
    ensure!(tree.free_nodes.len() == 3);

    Ok(())
}

#[test]
fn release_shared_extent() -> Result<()> {
    let nr_blocks = 1024;
    let nr_nodes = 3;

    let mut extents = Vec::new();
    let mut tree = Tree::new(nr_blocks, nr_nodes);
    extents.push(tree.borrow().unwrap());
    extents.push(tree.borrow().unwrap());
    extents.push(tree.borrow().unwrap());
    extents.push(tree.borrow().unwrap());

    tree.release(extents.remove(0));
    tree.release(extents.remove(0));

    Ok(())
}

#[test]
fn reuse_node() -> Result<()> {
    let nr_blocks = 1024;
    let nr_nodes = 3;

    let mut extents = Vec::new();
    let mut tree = Tree::new(nr_blocks, nr_nodes);
    extents.push(tree.borrow().unwrap());
    extents.push(tree.borrow().unwrap());
    ensure!(tree.free_nodes.len() == 0);

    // consume and release the left child
    {
        let mut extent = extents[0].lock().unwrap();
        extent.cursor = extent.end;
    }
    tree.release(extents.remove(0));
    ensure!(tree.free_nodes.len() == 2);

    // borrow a new extent that reuses the released nodes
    let ext = tree.borrow();
    ensure!(ext.is_some());
    extents.push(ext.unwrap());
    ensure!(tree.free_nodes.len() == 0);

    {
        let ext = extents[0].lock().unwrap();
        ensure!(ext.begin == 512);
        ensure!(ext.end == 768);
        ensure!(ext.cursor == 512);
    }

    {
        let ext = extents[1].lock().unwrap();
        ensure!(ext.begin == 768);
        ensure!(ext.end == 1024);
        ensure!(ext.cursor == 768);
    }

    Ok(())
}

#[test]
fn borrow_after_reset() -> Result<()> {
    let nr_blocks = 1024;
    let nr_nodes = 3;

    let mut extents = Vec::new();
    let mut tree = Tree::new(nr_blocks, nr_nodes);
    extents.push(tree.borrow().unwrap());
    extents.push(tree.borrow().unwrap());
    ensure!(tree.free_nodes.len() == 0);

    extents.clear();
    tree.reset();

    extents.push(tree.borrow().unwrap());
    ensure!(tree.free_nodes.len() == 2);
    {
        let ext = extents[0].lock().unwrap();
        ensure!(ext.begin == 0);
        ensure!(ext.end == nr_blocks);
        ensure!(ext.cursor == 0);
    }

    extents.push(tree.borrow().unwrap());
    ensure!(tree.free_nodes.len() == 0);
    let div = nr_blocks / 2;
    {
        let ext = extents[0].lock().unwrap();
        ensure!(ext.begin == 0);
        ensure!(ext.end == div);
        ensure!(ext.cursor == ext.begin);
    }
    {
        let ext = extents[1].lock().unwrap();
        ensure!(ext.begin == div);
        ensure!(ext.end == nr_blocks);
        ensure!(ext.cursor == ext.begin);
    }

    Ok(())
}

#[test]
fn borrow_after_resize() -> Result<()> {
    let nr_blocks = 1024;
    let nr_nodes = 3;

    let mut extents = Vec::new();
    let mut tree = Tree::new(nr_blocks, nr_nodes);
    extents.push(tree.borrow().unwrap());
    extents.push(tree.borrow().unwrap());
    ensure!(tree.free_nodes.len() == 0);

    extents.clear();
    let nr_blocks = 2048;
    tree.resize(nr_blocks);

    extents.push(tree.borrow().unwrap());
    ensure!(tree.free_nodes.len() == 2);
    {
        let ext = extents[0].lock().unwrap();
        ensure!(ext.begin == 0);
        ensure!(ext.end == nr_blocks);
        ensure!(ext.cursor == 0);
    }

    extents.push(tree.borrow().unwrap());
    ensure!(tree.free_nodes.len() == 0);
    let div = nr_blocks / 2;
    {
        let ext = extents[0].lock().unwrap();
        ensure!(ext.begin == 0);
        ensure!(ext.end == div);
        ensure!(ext.cursor == ext.begin);
    }
    {
        let ext = extents[1].lock().unwrap();
        ensure!(ext.begin == div);
        ensure!(ext.end == nr_blocks);
        ensure!(ext.cursor == ext.begin);
    }

    Ok(())
}

//----------------------------------------------------------------

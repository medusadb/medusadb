use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use gorgondb::{
    indexing::{BinaryTreePathElement, FixedSizeKey, TreeBranch},
    BlobId,
};

fn criterion_unit(c: &mut Criterion) {
    let mut tree_branch = TreeBranch::<BinaryTreePathElement, ()>::default();
    for i in 0..1024u128 {
        tree_branch.insert_non_existing(
            BinaryTreePathElement(i.to_bytes()),
            BlobId::self_contained(b"some value".as_slice()).unwrap(),
        );
    }

    let arc_tree_branch = Arc::new(tree_branch.clone());

    c.bench_function("TreeBranch::clone", |b| {
        b.iter(|| black_box(tree_branch.clone()))
    })
    .bench_function("Arc<TreeBranch>::clone", |b| {
        b.iter(|| black_box(arc_tree_branch.clone()))
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = criterion_unit
}
criterion_main!(benches);

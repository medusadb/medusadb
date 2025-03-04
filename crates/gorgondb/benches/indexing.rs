use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use gorgondb::{BlobId, Client, indexing::FixedSizeIndex};

async fn fixed_size_index() {
    let client = Client::new_for_tests();
    let tx = client.start_transaction("tx1").unwrap();

    let mut index = FixedSizeIndex::<u128>::new(tx);

    //index.min_count = 4;
    //index.max_count = 256; // This will force a factorization for each byte of the key.
    let blob_id =
        BlobId::self_contained(b"this is a rather large value that is self-contained".as_slice())
            .unwrap();

    for i in 0..1024 {
        index.insert(&i, blob_id.clone()).await.unwrap();
    }

    for i in 0..1024 {
        index.insert(&i, blob_id.clone()).await.unwrap();
    }

    for i in 0..1024 {
        index.remove(&i).await.unwrap();
    }
}

fn criterion_indexes(c: &mut Criterion) {
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();

    c.bench_function("fixed-size index", |b| {
        b.to_async(&tokio_rt).iter(fixed_size_index)
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(20));
    targets = criterion_indexes
}
criterion_main!(benches);

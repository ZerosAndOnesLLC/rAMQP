//! Codec + framing micro-benchmarks (criterion).
//!
//! ```sh
//! cargo bench --bench codec
//! ```

use bytes::{Bytes, BytesMut};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

use ramqp::Message;
use ramqp::codec::{from_slice, to_vec};
use ramqp::transport::frame::{decode_frame, encode_amqp_frame};
use ramqp::types::performatives::{Open, Performative, Transfer};

fn bench_message(c: &mut Criterion) {
    let msg = Message::data(Bytes::from(vec![0u8; 1024]));
    let encoded = to_vec(&msg);

    c.bench_function("encode_message_1k", |b| {
        b.iter(|| black_box(to_vec(black_box(&msg))))
    });
    c.bench_function("decode_message_1k", |b| {
        b.iter(|| {
            let m: Message = from_slice(black_box(&encoded)).unwrap();
            black_box(m)
        })
    });
}

fn bench_performative(c: &mut Criterion) {
    let open = Performative::Open(Open::new("bench-container"));
    let bytes = to_vec(&open);
    c.bench_function("encode_open", |b| {
        b.iter(|| black_box(to_vec(black_box(&open))))
    });
    c.bench_function("decode_open", |b| {
        b.iter(|| {
            let p: Performative = from_slice(black_box(&bytes)).unwrap();
            black_box(p)
        })
    });
}

fn bench_framing(c: &mut Criterion) {
    let transfer = Performative::Transfer(Transfer {
        handle: 0,
        delivery_id: Some(1),
        delivery_tag: Some(Bytes::from_static(b"tag")),
        ..Default::default()
    });
    let payload = vec![0u8; 1024];

    c.bench_function("encode_transfer_frame_1k", |b| {
        let mut buf = BytesMut::with_capacity(2048);
        b.iter(|| {
            buf.clear();
            encode_amqp_frame(&mut buf, 0, black_box(&transfer), Some(black_box(&payload)));
            black_box(&buf);
        })
    });

    let mut framed = BytesMut::new();
    encode_amqp_frame(&mut framed, 0, &transfer, Some(&payload));
    c.bench_function("decode_transfer_frame_1k", |b| {
        b.iter(|| {
            let mut src = framed.clone();
            let frame = decode_frame(&mut src, 1 << 20).unwrap();
            black_box(frame)
        })
    });
}

criterion_group!(benches, bench_message, bench_performative, bench_framing);
criterion_main!(benches);

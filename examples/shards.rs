//! Independent owners with one immutable encoded buffer shared across workers.
use rill_delivery::{DispatchBudget, Dispatcher, Message, PeerConfig};
use std::sync::Arc;

fn main() {
    let encoded: Arc<[u8]> = Arc::from([42u8; 64]);
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let encoded = encoded.clone();
            std::thread::spawn(move || {
                let mut sender = Dispatcher::<u64, Arc<[u8]>>::new(5000, 8 << 20).unwrap();
                // The outer handle is worker-local; the encoded inner bytes are shared.
                // Recipients do not contend on a single global reference counter.
                let message = Message::new(Arc::new(encoded), 128).unwrap();
                for _ in 0..5000 {
                    let peer = sender.add_peer(PeerConfig::default()).unwrap();
                    sender
                        .offer_state(peer, 7, 1, message.clone(), 0, 0)
                        .unwrap();
                }
                let mut out = Vec::new();
                sender
                    .dispatch_into(
                        1,
                        DispatchBudget {
                            bytes: 5000 * 128,
                            per_peer_bytes: 128,
                            deliveries: 5000,
                            visits: 5000,
                        },
                        &mut out,
                    )
                    .unwrap();
                let count = out.len();
                for envelope in out {
                    assert_eq!(&**envelope.delivery.message.payload, &[42u8; 64]);
                    // Simulated application confirmation. A real transport must wait
                    // for its recipient's ACK before advancing confirmed progress.
                    assert!(sender.ack(envelope.peer, envelope.delivery.receipt));
                }
                assert_eq!(sender.buffered_bytes(), 0);
                count
            })
        })
        .collect();
    let confirmed: usize = workers.into_iter().map(|w| w.join().unwrap()).sum();
    assert_eq!(confirmed, 20000);
    println!("confirmed {confirmed} deliveries across four independent owners");
}

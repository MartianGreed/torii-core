use resolve_path::PathResolveExt;
use std::path::PathBuf;
use torii::etl::event::EmittedEventExt;
use torii::etl::EventContext;
use torii_dojo::decoder::DojoDecoder;
use torii_dojo::manager::DojoTableStore;
use torii_dojo::DojoToriiError;
use torii_introspect_postgres_sink::processor::PostgresSimpleDb;
use torii_test_utils::{resolve_path_like, EventIterator, FakeProvider};

const DB_URL: &str = "postgres://user:password@localhost:5432/torii_test";
// const CHAIN_DATA_PATH: &str = "~/tc-tests/pistols";
const CHAIN_DATA_PATH: &str = "~/tc-tests/blob-arena";
const MANAGER_PATH: &str = "data/dojo-manager";

#[tokio::main]
async fn main() {
    let chain_path = resolve_path_like(CHAIN_DATA_PATH);
    let events_path = chain_path.join("events");
    let contracts_path = chain_path.join("model-contracts");
    // The JSON-backed manager is a local schema cache used by this example.
    let manager = DojoTableStore::new(&PathBuf::from(MANAGER_PATH).resolve().into_owned()).unwrap();
    let provider = FakeProvider::new(contracts_path);
    let event_iterator = EventIterator::new(events_path);
    let decoder = DojoDecoder::new_default(manager, provider);
    let mut db = PostgresSimpleDb::new(DB_URL, None).await.unwrap();
    db.initialize().await.unwrap();
    let context = EventContext::default();
    let mut success = 0;
    for (n, event) in event_iterator.enumerate() {
        match decoder.decode_raw_event(&event).await {
            Ok(msg) => match db.process_message(&msg, &context).await {
                Ok(_) => success += 1,
                Err(err) => {
                    let (selector, keys, data) = event.split_content().unwrap();
                    let parsed = decoder
                        .deserialize_dojo_event(selector, keys, data)
                        .unwrap();
                    println!(
                    "Failed to process message {n} {:?}:,\nevent: {:?}\nmessage: {:?}\n-------------",
                    err, parsed, msg
                )
                }
            },
            Err(DojoToriiError::UnknownDojoEventSelector(_)) => {
                println!("Unknown event selector, skipping event");
            }
            Err(err) => {
                println!("Failed to decode event: {:?}", err);
            }
        }
        if n % 1000 == 0 {
            println!("Decoded {n} events");
        }
    }
}

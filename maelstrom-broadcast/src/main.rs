// cargo build && ~/maelstrom.tar/maelstrom/maelstrom test -w broadcast --bin ./target/debug/maelstrom-broadcast --node-count 1 --time-limit 20 --rate 10
use std::collections::{BTreeSet, HashSet};
use async_trait::async_trait;
use maelstrom::protocol::{Message};
use maelstrom::{done, Node, Result, Runtime};
use std::sync::Arc;
use serde_json::{json};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::Mutex;

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let (tx, mut receiver) = channel(1);
    let handler = Arc::new(Handler { sender: tx, seen: Arc::new(Mutex::new(Vec::new())) });
    //let mut seen = HashSet::new();
    Runtime::new().with_handler(handler).run().await.unwrap();

    // let mut msg: u64 = 0;
    // thread::scope(|s| {
    //     s.spawn(|| {
    //         // This probably shouldn't be blocking?!
    //         msg = receiver.blocking_recv().unwrap();
    //         seen.insert(msg);
    //     });
    // });

    Ok(())
}

#[derive(Clone)]
struct Handler {
    sender: Sender<u64>,
    seen: Arc<Mutex<Vec<u64>>>,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        match req.get_type() {
            "broadcast" => Ok(process_broadcast_message(&self, req, runtime).await.unwrap()),
            "read" => Ok(process_read_message(&self, req, runtime).await.unwrap()),
            "topology" => Ok(process_topology_message(req, runtime).await.unwrap()),
            _ => return done(runtime, req)
        }
    }
}

async fn process_broadcast_message(handler: &Handler, req: Message, runtime: Runtime) -> Result<()> {
    let msg = req.body.extra.get("message").unwrap().as_number().unwrap().to_owned();
    let msg = msg.as_u64().unwrap();
    handler.seen.lock().await.push(msg);
    //handler.sender.send(msg).await.expect("Expected to store msg");
    let message_body = req.body.clone().with_type("broadcast_ok");
    runtime.reply(req, message_body).await
}

async fn process_read_message(handler: &Handler, req: Message, runtime: Runtime) -> Result<()> {
    let message_body = req.body.clone()
        .with_type("read_ok")
        .extra.insert(String::from("messages"), json!(handler.seen.lock().await.to_vec()));
    runtime.reply(req, message_body).await
}

async fn process_topology_message(req: Message, runtime: Runtime) -> Result<()> {
    let message_body = req.body.clone().with_type("topology_ok");
    runtime.reply(req, message_body).await
}

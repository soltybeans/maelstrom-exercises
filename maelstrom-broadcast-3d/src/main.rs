// cargo build && ~/maelstrom.tar/maelstrom/maelstrom test -w broadcast --bin ./target/debug/maelstrom-broadcast-3d --node-count 1 --time-limit 20 --rate 10
use async_trait::async_trait;
use maelstrom::protocol::{Message, MessageBody};
use maelstrom::{done, Node, Result, Runtime};
use std::sync::Arc;
use async_recursion::async_recursion;
use serde_json::{Value};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler { seen: Arc::new(Mutex::new(Vec::new())), neighbours: Arc::new(Mutex::new(Vec::new())) });
    Runtime::new().with_handler(handler).run().await.unwrap();

    Ok(())
}

#[derive(Clone)]
struct Handler {
    seen: Arc<Mutex<Vec<Value>>>,
    neighbours: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        match req.get_type() {
            "init" => {
                for i in runtime.neighbours() {
                    self.neighbours.lock().await.push(i.to_string());
                }
                Ok(())
            }
            "broadcast" => {
                let runtime = Arc::new(runtime);
                process_broadcast_message(&self, req, Arc::clone(&runtime)).await.unwrap();
                Ok(())
            }
            "read" => {
                process_read_message(&self, req, runtime).await.unwrap();
                Ok(())
            }
            "topology" => {
                process_topology_message(req, runtime).await.unwrap();
                Ok(())
            }
            _ => return done(runtime, req)
        }
    }
}

async fn process_broadcast_message(handler: &Handler, req: Message, runtime: Arc<Runtime>) -> Result<()> {
    let msg = req.body.extra.get("message").unwrap().as_number().unwrap().to_owned();
    handler.seen.lock().await.push(Value::from(msg));

    let mut neighbours;
    neighbours = handler.neighbours.lock().await.to_vec();

    // Copy the request once
    let request_for_reply: Message = Message { src: req.src.clone(), dest: req.dest.clone(), body: req.body.clone() };
    let req2 = Arc::new(req);
    let result = runtime.reply(request_for_reply, MessageBody::new().and_msg_id(req2.body.msg_id).with_type("broadcast_ok")).await;

    let mut set = JoinSet::new();
    for i in neighbours {
        let message = Arc::clone(&req2);
        let runtime_for_peer = Arc::clone(&runtime);
        let i_copy = Arc::new(i);
        let neighbour = Arc::clone(&i_copy);

        set.spawn(async move {
            if runtime_for_peer.rpc(i_copy.to_string(), message.body.raw()).await.is_err() {
                keep_calling(runtime_for_peer, neighbour, message).await
            }
        });
    }
    while let Some(res) = set.join_next().await {
        // Do nothing.
        res.unwrap()
    }

    result
}

#[async_recursion]
async fn keep_calling(runtime_for_peer: Arc<Runtime>, i: Arc<String>, r: Arc<Message>) {
    while runtime_for_peer.rpc(Arc::clone(&i).to_string(), r.body.raw()).await.is_err() {
        // Keep trying until network partition is resumed.
        let _ = keep_calling(Arc::clone(&runtime_for_peer), Arc::clone(&i), Arc::clone(&r)).await;
    }
}

async fn process_read_message(handler: &Handler, req: Message, runtime: Runtime) -> Result<()> {
    let mut message_body = MessageBody::new()
        .and_msg_id(req.body.msg_id)
        .with_type("read_ok");
    message_body.extra.insert(String::from("messages"), Value::Array(handler.seen.lock().await.clone()));
    runtime.reply(req, message_body).await
}

async fn process_topology_message(req: Message, runtime: Runtime) -> Result<()> {
    let msg = MessageBody::new()
        .and_msg_id(req.body.msg_id)
        .with_type("topology_ok");
    runtime.reply(req, msg).await
}

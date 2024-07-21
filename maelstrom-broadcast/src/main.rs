// cargo build && ~/maelstrom.tar/maelstrom/maelstrom test -w broadcast --bin ./target/debug/maelstrom-broadcast --node-count 1 --time-limit 20 --rate 10
use async_trait::async_trait;
use maelstrom::protocol::{Message, MessageBody};
use maelstrom::{done, Node, Result, Runtime};
use std::sync::Arc;
use serde_json::{Value};
use tokio::sync::Mutex;

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler { seen: Arc::new(Mutex::new(Vec::new())) });
    Runtime::new().with_handler(handler).run().await.unwrap();

    Ok(())
}

#[derive(Clone)]
struct Handler {
    seen: Arc<Mutex<Vec<Value>>>,
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
    handler.seen.lock().await.push(Value::from(msg));
    let message_body = MessageBody::new()
        .and_msg_id(req.body.msg_id)
        .with_type("broadcast_ok");
    runtime.reply(req, message_body).await
}

async fn process_read_message(handler: &Handler, req: Message, runtime: Runtime) -> Result<()> {
    let mut message_body = MessageBody::new()
        .and_msg_id(req.body.msg_id)
        .with_type("read_ok");
    message_body.extra.insert(String::from("messages"), serde_json::value::Value::Array(handler.seen.lock().await.clone()));
    runtime.reply(req, message_body).await
}

async fn process_topology_message(req: Message, runtime: Runtime) -> Result<()> {
    let msg = MessageBody::new()
        .and_msg_id(req.body.msg_id)
        .with_type("topology_ok");
    runtime.reply(req, msg).await
}

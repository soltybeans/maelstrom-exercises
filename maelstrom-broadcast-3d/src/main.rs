// cargo build && ~/maelstrom.tar/maelstrom/maelstrom test -w broadcast --bin ./target/debug/maelstrom-broadcast-3d --node-count 1 --time-limit 20 --rate 10
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
            "broadcast" => {
                process_broadcast_message(&self, req, runtime).await.unwrap();
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

async fn process_broadcast_message(handler: &Handler, req: Message, runtime: Runtime) -> Result<()> {
    let msg = req.body.extra.get("message").unwrap().as_number().unwrap().to_owned();
    handler.seen.lock().await.push(Value::from(msg));

    let runtime = Arc::new(runtime);
    // Works because of smart pointers.
    let iter_neighbours = runtime.neighbours();

    // Copy the request once
    let request_for_reply: Message = Message { src: req.src.clone(), dest: req.dest.clone(), body: req.body.clone() };
    let req2 = Arc::new(req);

    for i in iter_neighbours {
        let r = Arc::clone(&req2);

        let runtime_for_peer = Arc::clone(&runtime);
        // We can use one thread but interleave tasks while we're here to guarantee delivery to members
        // tokio::spawn is more expensive and requires cross-thread lifetimes ('static) but only necessary if we need parallelism.
        tokio::join!(async move {
            if runtime_for_peer.rpc(i, r.body.raw()).await.is_err() {
                while runtime_for_peer.rpc(i, r.body.raw()).await.is_err() {
                    // Keep trying until network partition is resumed.
                }
            }
        });
    }

    //reply takes over ownership of the request....
    runtime.reply(request_for_reply, MessageBody::new().and_msg_id(req2.body.msg_id).with_type("broadcast_ok")).await
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

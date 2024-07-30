// cargo build && ~/maelstrom.tar/maelstrom/maelstrom test -w broadcast --bin ./target/debug/maelstrom-broadcast-3d --node-count 1 --time-limit 20 --rate 10 --nemesis partition
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
    let handler = Arc::new(Handler {
        seen: Arc::new(Mutex::new(Vec::new())),
        backlog: Arc::new(Mutex::new(Default::default())),
        neighbours: Arc::new(Mutex::new(Vec::new())),
    });
    Runtime::new().with_handler(handler).run().await.unwrap();

    Ok(())
}

#[derive(Clone)]
struct Handler {
    seen: Arc<Mutex<Vec<Value>>>,
    neighbours: Arc<Mutex<Vec<String>>>,
    backlog: Arc<Mutex<JoinSet<()>>>,
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
    {
        handler.seen.lock().await.push(Value::from(msg.clone()));
    }
    let msg_id = req.body.msg_id;

    let neighbours = handler.neighbours.lock().await.to_vec();

    // Copy the request once
    let request_for_reply: Message = Message { src: req.src.clone(), dest: req.dest.clone(), body: req.body.clone() };
    let src = req.src.clone();

    let mut is_client = false;
    if src.contains('c') {
        is_client = true;
        // Do the replication only if the source is a client.
        // This would deem this node as the leader.
        for i in neighbours {
            let mut message_body = MessageBody::new()
                .with_type("broadcast")
                .and_msg_id(msg_id);
            message_body.extra.insert(String::from("message"), Value::from(msg.clone()));
            let message = Arc::new(Message {
                src: req.clone().dest,
                dest: i.clone(),
                body: message_body,
            });

            let runtime_for_peer = Arc::clone(&runtime);
            let i_copy = Arc::new(i);
            let neighbour = Arc::clone(&i_copy);

            // A backlog lasts as long as its handler.
            handler.backlog.lock().await.spawn(async move {
                keep_calling_neighbour(runtime_for_peer, neighbour, message).await;
            });
        }
    }

    let sync = tokio::join!(
        sync_messages_with_peers(handler, is_client),
    );
    if sync.0.is_err() {
        panic!("Error syncing messages with peers!");
    }
    let client_result = tokio::join!(
        reply_to_client(Arc::clone(&runtime), request_for_reply, req.clone())
    );
    client_result.0
}


async fn reply_to_client(runtime: Arc<Runtime>, message: Message, req: Message) -> Result<()> {
    let result = runtime.reply(message, MessageBody::new()
        .and_msg_id(req.clone().body.msg_id)
        .with_type("broadcast_ok"));
    result.await
}

async fn sync_messages_with_peers(handler: &Handler, is_client: bool) -> Result<()> {
    if !is_client {
        return Ok(());
    }
    while let Some(_res) = handler.backlog.lock().await.join_next().await {
        // Do nothing. Just collect results
    }
    Ok(())
}

#[async_recursion]
async fn keep_calling_neighbour(runtime_for_peer: Arc<Runtime>, i: Arc<String>, r: Arc<Message>) {
    // Might be something wrong here.
    // A partition may occur either as a dropped message or a delayed one. We need to handle both.
    let rpc_result = runtime_for_peer.rpc(Arc::clone(&i).to_string(), r.body.raw()).await;
    if rpc_result.is_ok() {
        let m = rpc_result.unwrap().await;
        if m.is_ok() && m.unwrap().get_type() == "broadcast_ok" {
            return;
        } else {
            keep_calling_neighbour(Arc::clone(&runtime_for_peer), Arc::clone(&i), Arc::clone(&r)).await;
        }
    } else {
        // give some time for the partition to heal
        //tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        keep_calling_neighbour(Arc::clone(&runtime_for_peer), Arc::clone(&i), Arc::clone(&r)).await;
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

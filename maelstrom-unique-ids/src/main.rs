// cargo build && ~/maelstrom.tar/maelstrom/maelstrom test -w unique-ids --bin ./target/debug/maelstrom-unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
use async_trait::async_trait;
use maelstrom::protocol::{Message};
use maelstrom::{done, Node, Result, Runtime};
use std::sync::Arc;
use uuid::{Uuid};

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Clone, Default)]
struct Handler {}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        if req.get_type() == "generate" {
            let id_string = generate_unique_id().await;
            let mut message_body = req.body.clone().with_type("generate_ok");
            let _ = message_body.extra.insert(String::from("id"), serde_json::value::Value::String(id_string));
            return runtime.reply(req, message_body).await;
        }
        done(runtime, req)
    }
}

async fn generate_unique_id() -> String {
    Uuid::new_v4().to_string()
}
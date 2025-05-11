use std::{io::Error, time::{SystemTime, UNIX_EPOCH}};

use prost::Message as ProstMessage;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    message::BorrowedMessage,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig, Message,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Stdout};
use uuid::Uuid;

pub mod analytics {
    include!(concat!(env!("OUT_DIR"), "/analytics.rs"));
}

use analytics::AnalyticsV1;

#[tokio::main]
async fn main() {
    let mut stdout = tokio::io::stdout();
    let mut input_lines = BufReader::new(tokio::io::stdin()).lines();

    let bootstrap_server = "localhost:9092";
    let topic = "events.test.producer";

    let producer = new_producer(bootstrap_server);
    let consumer = new_consumer(bootstrap_server, topic);

    loop {
        stdout.write_all(b"> ").await.unwrap();
        stdout.flush().await.unwrap();

        tokio::select! {
            message = consumer.recv() => {
                handle_message(&mut stdout, message).await;
            }
            line = input_lines.next_line() => {
                handle_input(&producer, topic, line).await;
            }
        }
    }
}

async fn handle_message(
    stdout_handle: &mut Stdout,
    message: Result<BorrowedMessage<'_>, KafkaError>,
) {
    let msg = message
        .expect("failed to read message")
        .detach();
    let bytes = msg.payload().unwrap();
    let payload = AnalyticsV1::decode(bytes)
        .expect("failed to deserialize analytics payload");

    stdout_handle.write_all(payload.payload.as_bytes()).await.unwrap();
    stdout_handle.write_all(b"\n").await.unwrap();
}

async fn handle_input(producer: &FutureProducer, topic: &str, line: Result<Option<String>, Error>) {
    if let Ok(Some(line)) = line {
        let payload = build_analytics_v1(&line);
        let bytes = payload.encode_to_vec();
        let record = FutureRecord::<(), _>::to(topic).payload(&bytes);
        producer
            .send(record, Timeout::Never)
            .await
            .expect("failed to produce record");
    }
}

fn build_analytics_v1(payload: &str) -> AnalyticsV1 {
    let now = SystemTime::now();
    let since = now.duration_since(UNIX_EPOCH)
        .expect("failed to convert to time since epoch");

    AnalyticsV1{
        event_id: "123".to_string(),
        event_name: "analytics.publish.v1".to_string(),
        service_name: "dispatch-play".to_string(),
        service_version: "0.1.0".to_string(),
        timestamp: Some(prost_types::Timestamp{
            nanos: since.subsec_nanos() as i32,
            seconds: since.as_secs() as i64,
        }),
        payload: payload.to_string(),
    }
}

fn new_producer(bootstrap_server: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("queue.buffering.max.ms", "0")
        .create()
        .expect("failed to create producer")
}

fn new_consumer(bootstrap_server: &str, topic: &str) -> StreamConsumer {
    let group_id = format!("consumer-{}", Uuid::new_v4());

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("enable.partition.eof", "false")
        .set("group.id", group_id)
        .create()
        .expect("failed to create consumer");

    consumer
        .subscribe(&[topic])
        .expect("failed to subscribe to topic");

    consumer
}

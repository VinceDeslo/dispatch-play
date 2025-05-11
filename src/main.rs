use std::io::Error;

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
    let msg = message.expect("failed to read message").detach();
    let payload = msg.payload().unwrap();
    stdout_handle.write_all(payload).await.unwrap();
    stdout_handle.write_all(b"\n").await.unwrap();
}

async fn handle_input(producer: &FutureProducer, topic: &str, line: Result<Option<String>, Error>) {
    if let Ok(Some(line)) = line {
        let record = FutureRecord::<(), _>::to(topic).payload(&line);
        producer
            .send(record, Timeout::Never)
            .await
            .expect("failed to produce record");
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

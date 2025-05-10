use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig, Message,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
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
                let message = message.expect("failed to read message").detach();
                let payload = message.payload().unwrap();
                stdout.write_all(payload).await.unwrap();
                stdout.write_all(b"\n").await.unwrap();
            }
            line = input_lines.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        let record = FutureRecord::<(), _>::to(topic)
                            .payload(&line);
                        producer.send(record, Timeout::Never)
                            .await
                            .expect("failed to produce record");
                    },
                    _ => break,
                }
            }
        }
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

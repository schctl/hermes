use std::io::ErrorKind;
use std::sync::atomic::AtomicBool;

use clap::Parser;
use hermes::link::Link;
use hermes::{topic, Node};
use tokio::io::{stdin, AsyncBufReadExt, BufReader, Stdin};
use tokio::time::Instant;
use tokio_serial::SerialStream;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Opts {
    #[arg(short, long)]
    serial_port: Option<String>,
    #[arg(short, long, default_value_t = 9600)]
    bps: u32,
}

fn open_serial_port(port: Option<String>, baud: u32) -> anyhow::Result<SerialStream> {
    let port = match port {
        Some(p) => p,
        _ => {
            tokio_serial::available_ports()?
                .into_iter()
                .next()
                .ok_or(anyhow::anyhow!("no port found"))?
                .port_name
        }
    };

    Ok(SerialStream::open(&tokio_serial::new(port, baud))?)
}

pub struct PortWrap(SerialStream);

impl Link for PortWrap {
    fn read(&mut self, buf: &mut [u8]) -> nb::Result<usize, ()> {
        match self.0.try_read(buf) {
            Ok(b) => Ok(b),
            Err(e) => match e.kind() {
                ErrorKind::WouldBlock => Err(nb::Error::WouldBlock),
                _ => Err(nb::Error::Other(())),
            },
        }
    }

    fn write(&mut self, buf: &[u8]) -> nb::Result<usize, ()> {
        match self.0.try_write(buf) {
            Ok(b) => Ok(b),
            Err(e) => match e.kind() {
                ErrorKind::WouldBlock => Err(nb::Error::WouldBlock),
                _ => Err(nb::Error::Other(())),
            },
        }
    }
}

async fn run(opts: Opts) -> anyhow::Result<()> {
    let mut port = PortWrap(open_serial_port(opts.serial_port, opts.bps)?);

    let mut node = Node::new_with_links(5, [&mut port]);

    let mut stdin = BufReader::new(stdin());

    let mut line = String::new();

    loop {
        if let Ok(_) = stdin.read_line(&mut line).await {
            let now = Instant::now();

            node.publish(topic::Message {
                id: 1,
                data: line.as_bytes(),
            })?
            .await;

            line.clear();

            let exit = AtomicBool::new(false);

            node.run(
                |message, queue| {
                    let data = core::str::from_utf8(message.data).unwrap();
                    println!("Received [rtt {}us] -> {}", now.elapsed().as_micros(), data);
                    exit.store(true, std::sync::atomic::Ordering::Relaxed);
                    async {}
                },
                &exit,
            )
            .await;
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Opts::parse();

    if let Err(e) = run(args).await {
        eprintln!("Error: {}", e);
        std::process::exit(-1);
    }
}

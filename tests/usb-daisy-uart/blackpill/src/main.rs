#![feature(impl_trait_in_assoc_type)]
#![feature(generic_arg_infer)]
#![no_std]
#![no_main]

extern crate alloc;

use core::sync::atomic::AtomicBool;

use defmt::*;
use embassy_executor::Spawner;
use embassy_futures::join::{join3, join4};
use embassy_futures::select::select;
use embassy_stm32::gpio::{AnyPin, Level, Output, Speed};
use embassy_stm32::time::Hertz;
use embassy_stm32::{rcc, Config};
use embassy_time::{Duration, Ticker, Timer};
use embassy_usb::class::cdc_acm::State;
use embedded_alloc::LlffHeap as Heap;
use {defmt_rtt as _, panic_probe as _};

use hermes::{topic, Node};
use hermes_link_embassy_eio::{read_loop, write_loop};
use hermes_link_embassy_rb::pipe::Pipe;
use hermes_link_embassy_usb::wrap;

mod uart;
mod usb;

#[global_allocator]
static HEAP: Heap = Heap::empty();

pub type Mutex<T> = embassy_sync::mutex::Mutex<embassy_sync::blocking_mutex::raw::NoopRawMutex, T>;

fn configure_heap() {
    use core::mem::MaybeUninit;
    use core::ptr::addr_of_mut;

    const HEAP_SIZE: usize = 2048;
    static mut HEAP_MEM: [MaybeUninit<u8>; HEAP_SIZE] = [MaybeUninit::uninit(); HEAP_SIZE];
    unsafe { HEAP.init(addr_of_mut!(HEAP_MEM) as usize, HEAP_SIZE) }
}

fn configure_rcc() -> rcc::Config {
    use embassy_stm32::rcc::*;

    let mut config = Config::default();

    config.hse = Some(Hse {
        freq: Hertz(25_000_000),
        mode: HseMode::Oscillator,
    });

    config.pll_src = PllSource::HSE;
    config.pll = Some(Pll {
        prediv: PllPreDiv::DIV25,
        mul: PllMul::MUL192,
        divp: Some(PllPDiv::DIV2),
        divq: Some(PllQDiv::DIV4),
        divr: None,
    });
    config.ahb_pre = AHBPrescaler::DIV1;
    config.apb1_pre = APBPrescaler::DIV2;
    config.apb2_pre = APBPrescaler::DIV1;
    config.sys = Sysclk::PLL1_P;

    config
}

#[embassy_executor::main]
async fn main(spawner: Spawner) {
    configure_heap();

    let mut config = Config::default();
    config.rcc = configure_rcc();

    let p = embassy_stm32::init(config);
    info!("Hello World!");

    spawner.spawn(set_dbg_mcu_sleep()).unwrap();
    spawner.spawn(keep_alive(p.PC13.into())).unwrap();

    spawner
        .spawn(echo_usb(
            usb::Peripherals {
                usb: p.USB_OTG_FS,
                dp: p.PA12,
                dm: p.PA11,
            },
            uart::Peripherals {
                uart: p.USART1,
                rx: p.PA10,
                tx: p.PA9,
                rx_dma: p.DMA2_CH2,
                tx_dma: p.DMA2_CH7,
            },
        ))
        .unwrap();
}

#[embassy_executor::task]
async fn keep_alive(p: AnyPin) -> ! {
    let mut led = Output::new(p, Level::High, Speed::Low);

    loop {
        led.set_high();
        Timer::after_millis(300).await;

        led.set_low();
        Timer::after_millis(300).await;
    }
}

#[embassy_executor::task]
async fn echo_usb(usb_p: usb::Peripherals, uart_p: uart::Peripherals) {
    // USB Setup

    let mut state = State::new();
    let mut usb_buffers: usb::Buffers = usb::Buffers::new();

    let (mut usb, class) = usb::create_usb(usb_p, &mut state, &mut usb_buffers);

    info!("Acquired USB device");

    let usb_fut = usb.run();

    let (mut class_s, mut class_r) = wrap(class);

    let usb_link = Pipe::<128>::new();
    let mut usb_link_2 = usb_link.clone();
    let (mut usb_link_s, mut usb_link_r) = usb_link.split();

    usb_link_2 = usb_link_2.flip();

    // Do stuff with the class!
    let echo_fut = async {
        loop {
            class_r.wait_connection().await;
            info!("Connected");

            let _ = select(
                read_loop::<_, 64, _>(&mut class_r, &mut usb_link_s),
                write_loop::<_, 64, _>(&mut class_s, &mut usb_link_r),
            )
            .await;
            info!("Disconnected");
        }
    };

    // UART Setup

    let mut dma_buf = [0; 128];

    let uart = uart::create_uart(uart_p).unwrap();
    let (mut uart_tx, uart_rx) = uart.split();

    info!("Acquired UART peripheral");

    let mut uart_rx = uart_rx.into_ring_buffered(&mut dma_buf);

    let uart_pipe = Pipe::<128>::new();

    let mut uart_pipe_2 = uart_pipe.clone();
    uart_pipe_2 = uart_pipe_2.flip();
    let (mut uart_link_s, mut uart_link_r) = uart_pipe.split();

    let uart_fut = async {
        loop {
            let _ = select(
                read_loop::<_, 64, _>(&mut uart_rx, &mut uart_link_s),
                write_loop::<_, 64, _>(&mut uart_tx, &mut uart_link_r),
            )
            .await;
        }
    };

    // Hermes Node

    let mut node = Node::new_with_links(0, [&mut usb_link_2, &mut uart_pipe_2]);

    let exit = AtomicBool::new(false);

    let node_fut = node.run(
        |message, queue| {
            let data = core::str::from_utf8(message.data).unwrap();
            info!("{}", data);

            // queue
            //     .push(topic::Message {
            //         id: 2,
            //         data: b"Hello from the node!",
            //     })
            //     .unwrap();
            async {}
        },
        &exit,
    );

    // Run everything concurrently.
    // If we had made everything `'static` above instead, we could do this using separate tasks instead.
    join4(usb_fut, echo_fut, node_fut, uart_fut).await;
}

// https://github.com/probe-rs/probe-rs/issues/2851
#[embassy_executor::task]
async fn set_dbg_mcu_sleep() {
    let mut ticker = Ticker::every(Duration::from_millis(500));

    loop {
        critical_section::with(|_cs| {
            embassy_stm32::pac::DBGMCU.cr().modify(|cr| {
                cr.set_dbg_sleep(true);
                cr.set_dbg_standby(true);
                cr.set_dbg_stop(true);
            })
        });

        ticker.next().await;
    }
}

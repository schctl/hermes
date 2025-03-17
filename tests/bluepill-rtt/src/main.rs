#![no_std]
#![no_main]
#![feature(impl_trait_in_assoc_type)]
#![feature(generic_arg_infer)]

extern crate alloc;

use core::sync::atomic::AtomicBool;

use defmt::*;
use embassy_executor::Spawner;
use embassy_futures::join::join3;
use embassy_futures::select::select;
use embassy_stm32::gpio::{AnyPin, Level, Output, Speed};
use embassy_stm32::time::Hertz;
use embassy_stm32::{rcc, Config};
use embassy_time::Timer;
use embassy_usb::class::cdc_acm::State;
use embedded_alloc::LlffHeap as Heap;

use hermes::{topic, Node};
use hermes_link_embassy_rb::pipe::Pipe;
use hermes_link_embassy_eio::{read_loop, write_loop};
use hermes_link_embassy_usb::wrap;

use {defmt_rtt as _, panic_probe as _};

pub mod usb;

#[global_allocator]
static HEAP: Heap = Heap::empty();

fn configure_heap() {
    use core::mem::MaybeUninit;
    use core::ptr::addr_of_mut;

    const HEAP_SIZE: usize = 2048;
    static mut HEAP_MEM: [MaybeUninit<u8>; HEAP_SIZE] = [MaybeUninit::uninit(); HEAP_SIZE];
    unsafe { HEAP.init(addr_of_mut!(HEAP_MEM) as usize, HEAP_SIZE) }
}

fn configure_rcc() -> rcc::Config {
    use embassy_stm32::rcc::*;

    let mut rcc = Config::default();

    rcc.hse = Some(Hse {
        freq: Hertz(8_000_000),
        mode: HseMode::Oscillator,
    });

    rcc.pll = Some(Pll {
        src: PllSource::HSE,
        prediv: PllPreDiv::DIV1,
        mul: PllMul::MUL9,
    });
    rcc.ahb_pre = AHBPrescaler::DIV1;
    rcc.apb1_pre = APBPrescaler::DIV2;
    rcc.apb2_pre = APBPrescaler::DIV1;
    rcc.sys = Sysclk::PLL1_P;

    rcc
}

#[embassy_executor::main]
async fn main(spawner: Spawner) {
    configure_heap();

    let mut config = Config::default();
    config.rcc = configure_rcc();

    let p = embassy_stm32::init(config);
    info!("Hello World!");

    spawner.spawn(keep_alive(p.PB2.into())).unwrap();
    spawner
        .spawn(echo_usb(usb::Peripherals {
            usb: p.USB,
            dp: p.PA12,
            dm: p.PA11,
        }))
        .unwrap();

    loop {
        Timer::after_micros(100).await;
    }
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
async fn echo_usb(p: usb::Peripherals) {
    let mut state = State::new();
    let mut usb_buffers: usb::Buffers = usb::Buffers::new();

    let (mut usb, class) = usb::create_usb(p, &mut state, &mut usb_buffers);

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

    let mut node = Node::new_with_links(0, [&mut usb_link_2]);

    let exit = AtomicBool::new(false);

    let node_fut = node.run(
        |message, queue| {
            let data = core::str::from_utf8(message.data).unwrap();
            info!("{}", data);

            queue
                .push(topic::Message {
                    id: 2,
                    data: b"Hello from the node!",
                })
                .unwrap();
            async {}
        },
        &exit,
    );

    // Run everything concurrently.
    // If we had made everything `'static` above instead, we could do this using separate tasks instead.
    join3(usb_fut, echo_fut, node_fut).await;
}

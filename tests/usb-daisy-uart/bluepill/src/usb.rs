//! USB configuration.

use embassy_stm32::peripherals::{PA11, PA12, USB};
use embassy_stm32::{bind_interrupts, peripherals, usb};
use embassy_time::Timer;
use embassy_usb::class::cdc_acm::{CdcAcmClass, State};
use embassy_usb::{Builder, UsbDevice};

bind_interrupts!(pub struct Irqs {
    USB_LP_CAN1_RX0 => usb::InterruptHandler<peripherals::USB>;
});

pub type Driver<'a> = embassy_stm32::usb::Driver<'a, USB>;

const USB_CONFIG: embassy_usb::Config = {
    let mut config = embassy_usb::Config::new(0x0bed, 0xb0aa);

    config.manufacturer = Some("Project MANAS");
    config.product = Some(env!("CARGO_CRATE_NAME"));
    config.serial_number = Some(env!("CARGO_PKG_VERSION"));

    // Required for windows compatibility.
    // https://developer.nordicsemi.com/nRF_Connect_SDK/doc/1.9.1/kconfig/CONFIG_CDC_ACM_IAD.html#help
    config.device_class = 0xEF;
    config.device_sub_class = 0x02;
    config.device_protocol = 0x01;
    config.composite_with_iads = true;

    config
};

pub struct Peripherals {
    pub usb: USB,
    pub dp: PA12,
    pub dm: PA11,
}

#[derive(Debug, Clone)]
pub struct Buffers<
    const EI: usize = 256,
    const CD: usize = 256,
    const BD: usize = 256,
    const MD: usize = 256,
    const CB: usize = 64,
> {
    ep_intermediate: [u8; EI],
    config_descriptor: [u8; CD],
    bos_descriptor: [u8; BD],
    msos_descriptor: [u8; MD],
    control_buf: [u8; CB],
}

impl Buffers {
    pub const fn new() -> Self {
        Self {
            ep_intermediate: [0; 256],
            config_descriptor: [0; 256],
            bos_descriptor: [0; 256],
            msos_descriptor: [0; 256],
            control_buf: [0; 64],
        }
    }
}

impl Default for Buffers {
    fn default() -> Self {
        Self::new()
    }
}

fn create_driver<'a>(p: Peripherals, buffer: &'a mut [u8]) -> Driver<'a> {
    Driver::new(p.usb, Irqs, p.dp, p.dm)
}

fn reset_usb_blocking(dp: &mut PA12) {
    use embassy_stm32::gpio::{Level, Output, Speed};

    // BluePill board has a pull-up resistor on the D+ line.
    // Pull the D+ pin down to send a RESET condition to the USB bus.
    // This forced reset is needed only for development, without it host
    // will not reset your device when you upload new firmware.
    let _dp = Output::new(dp, Level::Low, Speed::Low);

    embassy_futures::block_on(Timer::after_millis(10));
}

pub fn create_usb<'a>(
    mut p: Peripherals,
    state: &'a mut State<'a>,
    buffers: &'a mut Buffers,
) -> (UsbDevice<'a, Driver<'a>>, CdcAcmClass<'a, Driver<'a>>) {
    // reset_usb_blocking(&mut p.dp);

    let driver = create_driver(p, &mut buffers.ep_intermediate);

    let mut builder = Builder::new(
        driver,
        USB_CONFIG,
        &mut buffers.config_descriptor,
        &mut buffers.bos_descriptor,
        &mut buffers.msos_descriptor,
        &mut buffers.control_buf,
    );

    let class = CdcAcmClass::new(&mut builder, state, 64);
    let device = builder.build();

    (device, class)
}

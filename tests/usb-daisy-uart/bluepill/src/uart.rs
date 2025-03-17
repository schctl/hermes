use embassy_stm32::bind_interrupts;
use embassy_stm32::mode::Async;
use embassy_stm32::peripherals::{self, DMA1_CH4, DMA1_CH5, PA10, PA9, USART1};
use embassy_stm32::usart::{self, Config, ConfigError, Uart};

bind_interrupts!(struct Irqs {
    USART1 => usart::InterruptHandler<peripherals::USART1>;
});

pub struct Peripherals {
    pub uart: USART1,
    pub rx: PA10,
    pub tx: PA9,
    pub rx_dma: DMA1_CH5,
    pub tx_dma: DMA1_CH4,
}

pub fn create_uart<'d>(p: Peripherals) -> Result<Uart<'d, Async>, ConfigError> {
    let mut config = Config::default();
    config.baudrate = 9600;

    Uart::new(p.uart, p.rx, p.tx, Irqs, p.tx_dma, p.rx_dma, config)
}

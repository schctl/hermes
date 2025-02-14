#![no_std]

use derive_more::{Deref, DerefMut, From};

use embassy_usb::class::cdc_acm::{CdcAcmClass, Receiver, Sender};
use embassy_usb::driver::{Driver, EndpointError};

pub fn wrap<'a, D: Driver<'a>>(class: CdcAcmClass<'a, D>) -> (TxWrap<'a, D>, RxWrap<'a, D>) {
    let (tx, rx) = class.split();
    (TxWrap(tx), RxWrap(rx))
}

#[derive(Debug, From, Deref, DerefMut)]
#[repr(transparent)]
pub struct EndpointErrorWrap(EndpointError);

impl embedded_io_async::Error for EndpointErrorWrap {
    fn kind(&self) -> embedded_io_async::ErrorKind {
        match self.0 {
            EndpointError::BufferOverflow => embedded_io_async::ErrorKind::OutOfMemory,
            EndpointError::Disabled => embedded_io_async::ErrorKind::NotConnected,
        }
    }
}

#[derive(From, Deref, DerefMut)]
#[repr(transparent)]
pub struct RxWrap<'a, D: Driver<'a>>(Receiver<'a, D>);

impl<'a, D: Driver<'a>> embedded_io_async::ErrorType for RxWrap<'a, D> {
    type Error = EndpointErrorWrap;
}

impl<'a, D: Driver<'a>> embedded_io_async::Read for RxWrap<'a, D> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.0.read_packet(buf).await.map_err(EndpointErrorWrap)
    }
}

#[derive(From, Deref, DerefMut)]
#[repr(transparent)]
pub struct TxWrap<'a, D: Driver<'a>>(Sender<'a, D>);

impl<'a, D: Driver<'a>> embedded_io_async::ErrorType for TxWrap<'a, D> {
    type Error = EndpointErrorWrap;
}

impl<'a, D: Driver<'a>> embedded_io_async::Write for TxWrap<'a, D> {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        let written = buf.len();
        self.0
            .write_packet(buf)
            .await
            .map_err(EndpointErrorWrap)
            .map(|_| written)
    }
}

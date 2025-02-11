#![no_std]

use core::future::poll_fn;
use core::task::Poll;

use embassy_usb::class::cdc_acm::{Receiver, Sender};
use embassy_usb::driver::{Driver, EndpointError};
use hermes::link::Link;
use hermes_link_embassy_rb::SharedRingBuffer;

pub async fn read_loop<'a, D: Driver<'a>, const N: usize, const PKT: usize>(
    cdc_rx: &mut Receiver<'a, D>,
    pipe_tx: &mut SharedRingBuffer<N>,
) -> Result<(), EndpointError> {
    let mut read_buf = [0; PKT];

    loop {
        let n = cdc_rx.read_packet(&mut read_buf).await?;

        let mut written = 0;

        while written < n {
            let bytes = poll_fn(|c| {
                if let Ok(bytes) = pipe_tx.write(&read_buf[written..n]) {
                    if bytes > 0 {
                        return Poll::Ready(bytes);
                    }
                }

                c.waker().wake_by_ref();
                Poll::Pending
            })
            .await;

            written += bytes;
        }
    }
}

pub async fn write_loop<'a, D: Driver<'a>, const N: usize, const PKT: usize>(
    cdc_tx: &mut Sender<'a, D>,
    pipe_rx: &mut SharedRingBuffer<N>,
) -> Result<(), EndpointError> {
    let mut write_buf = [0; PKT];

    loop {
        let bytes = poll_fn(|c| {
            if let Ok(bytes) = pipe_rx.read(&mut write_buf) {
                if bytes > 0 {
                    return Poll::Ready(bytes);
                }
            }

            c.waker().wake_by_ref();
            Poll::Pending
        })
        .await;

        cdc_tx.write_packet(&write_buf[..bytes]).await?;
    }
}

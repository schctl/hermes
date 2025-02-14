#![no_std]

use core::future::poll_fn;
use core::task::Poll;

use hermes::link::Link;
use hermes_link_embassy_rb::SharedRingBuffer;

pub async fn read_loop<const N: usize, const PKT: usize, T>(
    mut data_rx: T,
    pipe_tx: &mut SharedRingBuffer<N>,
) -> Result<(), T::Error>
where
    T: embedded_io_async::Read,
{
    let mut read_buf = [0; PKT];

    loop {
        let n = data_rx.read(&mut read_buf).await?;

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

pub async fn write_loop<const N: usize, const PKT: usize, T>(
    mut data_tx: T,
    pipe_rx: &mut SharedRingBuffer<N>,
) -> Result<(), T::Error>
where
    T: embedded_io_async::Write,
{
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

        data_tx.write_all(&write_buf[..bytes]).await?;
    }
}

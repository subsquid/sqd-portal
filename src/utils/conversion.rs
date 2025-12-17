use anyhow::anyhow;
use async_compression::tokio::bufread::{GzipDecoder, GzipEncoder};
use async_stream::stream;
use futures::{pin_mut, Stream, StreamExt};
use hex_literal::hex;
use itertools::Itertools;
use libz_ng_sys::crc32_combine;
use tokio::io::BufReader;
use tokio_util::{
    bytes::Bytes,
    io::{ReaderStream, StreamReader},
};

use crate::utils::gz_utils::GzStreamHolder;

const JOIN_GZIP_CHUNK_SIZE: usize = 32 * 1024;

#[allow(unstable_name_collisions)]
pub fn json_lines_to_json(data: &[u8]) -> anyhow::Result<Vec<u8>> {
    use std::io::{Read, Write};
    let mut reader = flate2::bufread::GzDecoder::new(data);
    let mut json_lines = String::new();
    reader.read_to_string(&mut json_lines)?;
    let mut writer = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    writer.write_all("[".as_bytes())?;
    for chunk in json_lines.trim_end().lines().intersperse(",") {
        writer.write_all(chunk.as_bytes())?;
    }
    writer.write_all("]".as_bytes())?;
    Ok(writer.finish()?)
}

#[allow(dead_code)]
pub fn recompress_gzip<S>(stream: S) -> impl futures::Stream<Item = std::io::Result<Bytes>>
where
    S: futures::Stream<Item = Vec<u8>>,
{
    let reader =
        StreamReader::new(stream.map(|result| std::io::Result::Ok(Bytes::from_owner(result))));

    let mut decoder = GzipDecoder::new(reader);
    decoder.multiple_members(true);

    let encoder =
        GzipEncoder::with_quality(BufReader::new(decoder), async_compression::Level::Fastest);

    ReaderStream::new(encoder)
}

pub fn join_gzip_default<S: Stream<Item = Vec<u8>>>(
    data: S,
) -> impl Stream<Item = Result<Bytes, anyhow::Error>> {
    join_gzip(data, JOIN_GZIP_CHUNK_SIZE)
}

pub fn join_gzip<S: Stream<Item = Vec<u8>>>(
    data: S,
    gzip_chunk_size: usize,
) -> impl Stream<Item = Result<Bytes, anyhow::Error>> {
    stream! {
        let mut crc = unsafe { libz_ng_sys::crc32(0, std::ptr::null(), 0) };
        let mut tot = 0u32;
        let buf = hex!("1f8b08000000000000ff");
        yield Ok(buf.to_vec().into());
        pin_mut!(data);
        let mut input = GzStreamHolder::new(data, gzip_chunk_size);

        loop {
            let _ = match input.gzhead().await {
                Ok(res) => { res },
                Err(_) => {
                    yield Ok([&[3u8, 0u8], &crc.to_le_bytes()[..], &tot.to_le_bytes()[..]].concat().into());
                    break;
                },
            };

            input.init()?;

            /* inflate and copy compressed data, clear last-block bit if requested */
            let mut len = 0;
            input.zpull().await?;
            let mut last = input.clear_last_flag();
            loop {
                /* if input used and output done, write used input and get more */
                if !input.can_read_more() && input.can_write_more() {
                    let buffer = input.copy_processed_buffer(true)?;
                    yield Ok(buffer.into());
                    input.reset_and_pull().await?;
                }
                /* decompress -- return early when end-of-block reached */
                /* update length of uncompressed data */
                len += input.decompress_availble_data()?;

                /* check for block boundary (only get this when block copied out) */
                if input.is_block_boundary() {
                    /* if that was the last block, then done */
                    if last {
                        break;
                    }
                    /* number of unused bits in last byte */
                    let pos = input.get_unused_bits();
                    /* find the next last-block bit */
                    if pos != 0 {
                        /* next last-block bit is in last used byte */
                        last = input.clear_last_flag_with_unused_bits(pos);
                    } else {
                        /* next last-block bit is in next unused byte */
                        if !input.can_read_more() {
                            /* don't have that byte yet -- get it */
                            let buffer = input.copy_processed_buffer(true)?;
                            yield Ok(buffer.into());
                            input.reset_and_pull().await?
                        }
                        last = input.clear_last_flag();
                    }
                } else {
                    if input.can_read_more() {
                        let buffer = input.copy_processed_buffer(false)?;
                        yield Ok(buffer.into());
                    }
                }
            }

            /* update buffer with unused input */
            input.set_read_ptr();
            /* copy used input, write empty blocks to get to byte boundary */
            let pos = input.get_unused_bits();
            let buffer = input.copy_processed_buffer(false)?;
            yield Ok(buffer.into());

            let mut last_byte = input.get_last_byte();
            if pos == 0 {
                /* already at byte boundary, or last file: write last byte */
                yield Ok(vec![last_byte].into());
            } else {
                /* append empty blocks to last byte */
                last_byte &= (0x100u16 >> pos) as u8 - 1; /* assure unused bits are zero */
                if pos & 1 > 0 {
                    if pos == 1 {
                        yield Ok(vec![last_byte, 0, 0, 0, 0xff, 0xff].into());
                    } else {
                        yield Ok(vec![last_byte, 0, 0, 0xff, 0xff].into());
                    }
                } else {
                    /* even -- append 1, 2, or 3 empty fixed blocks */
                    match pos {
                        6 => {
                            yield Ok(vec![last_byte | 8, 0x20, 0x80, 0].into());
                        }
                        4 => {
                            yield Ok(vec![last_byte | 0x20, 0x80, 0].into());
                        }
                        2 => {
                            yield Ok(vec![last_byte | 0x80, 0].into());
                        }
                        _ => {
                            yield Err(anyhow!("Incorrect POS"));
                            return;
                        }
                    }
                }
            }

            /* update crc and tot */
            crc = unsafe { crc32_combine(crc, input.get4().await?, len.try_into()?) };
            let _ = input.get4().await?;
            tot += len as u32;
            /* clean up */
            input.cleanup();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};
    use futures::{future::join_all, lock::Mutex};

    use flate2::bufread::GzDecoder;
    use rand::RngCore;
    use std::io::Read;
    use tokio::{io::AsyncBufReadExt, time::sleep};
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    // use core::slice::SlicePattern;
    

    use super::*;

    fn generate_data(data: Vec<Vec<u8>>, first_fragment_len: usize) -> impl Stream<Item = Vec<u8>> {
        stream! {
            for datum in data {
                let encoder = GzipEncoder::with_quality(&datum[..], async_compression::Level::Fastest);
                let buf = BufReader::with_capacity(100_000_000, encoder).fill_buf().await.unwrap().to_vec();
                if buf.len() > first_fragment_len {
                    yield buf[..first_fragment_len].to_vec();
                    yield buf[first_fragment_len..].to_vec();
                } else {
                    yield buf;
                }
            }
        }
    }

    fn stream_data(data: Vec<Vec<u8>>, first_fragment_len: usize) -> impl Stream<Item = Vec<u8>> {
        stream! {
            for datum in data {
                if datum.len() > first_fragment_len {
                    yield datum[..first_fragment_len].to_vec();
                    yield datum[first_fragment_len..].to_vec();
                } else {
                    yield datum;
                }
            }
        }
    }

    async fn pack_join_unpack(testcase: Vec<Vec<u8>>, chunk_size: usize) {
        let gt = testcase.concat();
        let stream = join_gzip(Box::pin(generate_data(testcase, 100_000)), chunk_size);
        pin_mut!(stream);
        let reader = StreamReader::new(
            stream.map(|result| std::io::Result::Ok(Bytes::from_owner(result.unwrap()))),
        );
        let decoder = GzipDecoder::new(reader);
        let mut buf_reader = BufReader::with_capacity(100_000_000, decoder);
        let result = buf_reader.fill_buf().await.unwrap();
        assert_eq!(result.to_vec(), gt);
    }

    async fn hierarchical_join_unpack(testcase: Vec<Vec<u8>>, final_fragmentation: usize) {
        let gt = testcase.concat();
        let len_test = testcase.len();
        assert!(len_test > 3);
        let stream = join_gzip_default(Box::pin(generate_data(testcase[..len_test / 2].to_vec(), 100_000)));
        pin_mut!(stream);
        let mut part_a = Vec::default();
        while let Some(Ok(data)) = stream.next().await {
            part_a.extend(data);
        }
        let stream = join_gzip_default(Box::pin(generate_data(testcase[len_test / 2..].to_vec(), 100_000)));
        pin_mut!(stream);
        let mut part_b = Vec::default();
        while let Some(Ok(data)) = stream.next().await {
            part_b.extend(data);
        }
        let stream = join_gzip_default(Box::pin(stream_data([part_a, part_b].to_vec(), final_fragmentation)));
        pin_mut!(stream);
        let reader = StreamReader::new(
            stream.map(|result| std::io::Result::Ok(Bytes::from_owner(result.unwrap()))),
        );
        let decoder = GzipDecoder::new(reader);
        let mut buf_reader = BufReader::with_capacity(100_000_000, decoder);
        let result = buf_reader.fill_buf().await.unwrap();
        assert_eq!(result.to_vec(), gt);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 200)]
    async fn test_forced_async() {
        let input = vec![vec![1u8, 2], vec![3, 4]];
        let gt = input.concat();
        let stream = Arc::new(Mutex::new(Box::pin(join_gzip_default(Box::pin(generate_data(input, 10))))));
        let vec = Arc::new(Mutex::new(Vec::<u8>::default()));
        
        let mut futures = Vec::new();
        for _ in 0..10 {
            let local_stream = Arc::clone(&stream);
            let local_vec =  Arc::clone(&vec);
            let future = tokio::spawn(async move {
                let millis;
                {
                    let mut rng = rand::rng();
                    millis = rng.next_u64() % 100;
                }
                sleep(Duration::from_millis(millis)).await;
                let mut locked_stream = local_stream.lock().await;
                match locked_stream.next().await {
                    Some(Ok(result)) => {
                        local_vec.lock().await.extend(result);
                    },
                    _ => {},
                };
            });
            futures.push(future);
        }
        join_all(futures).await;
        let data = (*vec.lock().await).clone();
        let mut buf = Vec::<u8>::default();
        let mut decoder = GzDecoder::new(data.as_slice());
        decoder.read_to_end(&mut buf).unwrap();

        assert_eq!(gt, buf);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 200)]
    async fn test_forced_async_long() {
        let seed: u64 = 42;
        let mut rng = StdRng::seed_from_u64(seed);
        let input = (0..9)
            .map(|_| {
                let mut data = [0u8; 1000];
                rng.fill_bytes(&mut data);
                data.to_vec()
            })
            .collect::<Vec<Vec<u8>>>();
        let gt = input.concat();
        let stream = Arc::new(Mutex::new(Box::pin(join_gzip_default(Box::pin(generate_data(input, 10))))));
        let vec = Arc::new(Mutex::new(Vec::<u8>::default()));
        
        let mut futures = Vec::new();
        for _ in 0..100 {
            let local_stream = Arc::clone(&stream);
            let local_vec =  Arc::clone(&vec);
            let future = tokio::spawn(async move {
                let millis;
                {
                    let mut rng = rand::rng();
                    millis = rng.next_u64() % 100;
                }
                sleep(Duration::from_millis(millis)).await;
                let mut locked_stream = local_stream.lock().await;
                match locked_stream.next().await {
                    Some(Ok(result)) => {
                        local_vec.lock().await.extend(result);
                    },
                    _ => {},
                };
            });
            futures.push(future);
        }
        join_all(futures).await;
        let data = (*vec.lock().await).clone();
        let mut buf = Vec::<u8>::default();
        let mut decoder = GzDecoder::new(data.as_slice());
        decoder.read_to_end(&mut buf).unwrap();

        assert_eq!(gt, buf);
    }

    #[tokio::test]
    async fn test_smoke() {
        let input = vec![vec![1u8, 2], vec![3, 4]];
        pack_join_unpack(input, JOIN_GZIP_CHUNK_SIZE).await;
    }

    #[tokio::test]
    async fn test_hierarchical_smoke() {
        let input = vec![vec![1u8, 2], vec![3, 4], vec![5, 6], vec![7, 8],];
        hierarchical_join_unpack(input, 10_000).await;
    }

    #[tokio::test]
    async fn test_hierarchical_exact_block_boundary() {
        let input = vec![vec![1u8, 2, 1, 1, 1, 1, 1, 1, 1, 1], vec![3, 4], vec![5, 6], vec![7, 8],];
        hierarchical_join_unpack(input, 17).await;
    }

    #[tokio::test]
    async fn test_const() {
        let input = vec![vec![1u8; 100_000], vec![3; 100_000]];
        pack_join_unpack(input, JOIN_GZIP_CHUNK_SIZE).await;
    }

    #[tokio::test]
    async fn test_const_small_chunk() {
        let input = vec![vec![1u8; 100_000], vec![3; 100_000]];
        pack_join_unpack(input, 128).await;
    }

    #[tokio::test]
    async fn test_rand_many() {
        let seed: u64 = 42;
        let mut rng = StdRng::seed_from_u64(seed);
        let input = (0..99)
            .map(|_| {
                let mut data = [0u8; 100_000];
                rng.fill_bytes(&mut data);
                data.to_vec()
            })
            .collect();
        pack_join_unpack(input, JOIN_GZIP_CHUNK_SIZE).await;
    }

    #[tokio::test]
    async fn test_rand_long() {
        let seed: u64 = 42;
        let mut rng = StdRng::seed_from_u64(seed);
        let input = (0..9)
            .map(|_| {
                let mut data = [0u8; 1_000_000];
                rng.fill_bytes(&mut data);
                data.to_vec()
            })
            .collect();
        pack_join_unpack(input, JOIN_GZIP_CHUNK_SIZE).await;
    }

    proptest_async::proptest! {
        #[test]
        async fn random_join(input in proptest::collection::vec(proptest::collection::vec(0..255u8, 10000..40000), 1..5)) {
            pack_join_unpack(input, JOIN_GZIP_CHUNK_SIZE).await;
        }
    }
}

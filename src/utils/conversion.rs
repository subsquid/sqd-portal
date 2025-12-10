use std::slice;

use anyhow::anyhow;
use async_compression::tokio::bufread::{GzipDecoder, GzipEncoder};
use async_stream::stream;
use futures::{pin_mut, Stream, StreamExt};
use hex_literal::hex;
use itertools::Itertools;
use libz_ng_sys::{
    crc32_combine, inflate, inflateEnd, inflateInit2_, z_stream, Z_BLOCK, Z_DATA_ERROR,
    Z_MEM_ERROR, Z_OK,
};
use tokio::io::BufReader;
use tokio_util::{
    bytes::Bytes,
    io::{ReaderStream, StreamReader},
};

const CHUNK: usize = 32768;
// const CHUNK: usize = 128;

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

mod allocator {
    use libz_ng_sys::uInt;

    use std::alloc::{self, Layout};
    use std::convert::TryFrom;
    use std::os::raw::c_void;
    use std::ptr;

    const ALIGN: usize = std::mem::align_of::<usize>();

    fn align_up(size: usize, align: usize) -> usize {
        (size + align - 1) & !(align - 1)
    }

    pub extern "C" fn zalloc(_ptr: *mut c_void, items: uInt, item_size: uInt) -> *mut c_void {
        // We need to multiply `items` and `item_size` to get the actual desired
        // allocation size. Since `zfree` doesn't receive a size argument we
        // also need to allocate space for a `usize` as a header so we can store
        // how large the allocation is to deallocate later.
        let size = match items
            .checked_mul(item_size)
            .and_then(|i| usize::try_from(i).ok())
            .map(|size| align_up(size, ALIGN))
            .and_then(|i| i.checked_add(std::mem::size_of::<usize>()))
        {
            Some(i) => i,
            None => return ptr::null_mut(),
        };

        // Make sure the `size` isn't too big to fail `Layout`'s restrictions
        let layout = match Layout::from_size_align(size, ALIGN) {
            Ok(layout) => layout,
            Err(_) => return ptr::null_mut(),
        };

        unsafe {
            // Allocate the data, and if successful store the size we allocated
            // at the beginning and then return an offset pointer.
            let ptr = alloc::alloc(layout) as *mut usize;
            if ptr.is_null() {
                return ptr as *mut c_void;
            }
            *ptr = size;
            ptr.add(1) as *mut c_void
        }
    }

    pub extern "C" fn zfree(_ptr: *mut c_void, address: *mut c_void) {
        unsafe {
            // Move our address being freed back one pointer, read the size we
            // stored in `zalloc`, and then free it using the standard Rust
            // allocator.
            let ptr = (address as *mut usize).offset(-1);
            let size = *ptr;
            let layout = Layout::from_size_align_unchecked(size, ALIGN);
            alloc::dealloc(ptr as *mut u8, layout)
        }
    }
}

pub struct StreamIn<S: Stream<Item = Vec<u8>> + Unpin> {
    stream: S,
    left: usize,
    next: *mut u8,
    buf: Vec<u8>,
    skip: usize,
}

impl<S: Stream<Item = Vec<u8>> + Unpin> StreamIn<S> {
    fn new(stream: S) -> Self {
        Self {
            stream,
            left: 0,
            next: std::ptr::null_mut(),
            buf: Default::default(),
            skip: 0,
        }
    }

    async fn load(&mut self) -> Result<(), anyhow::Error> {
        self.buf = self
            .stream
            .next()
            .await
            .ok_or(anyhow!("faild to load data from incoming stream"))?;
        self.left = self.buf.len();
        self.next = self.buf.as_mut_ptr();
        Ok(())
    }

    async fn get(&mut self) -> Result<u8, anyhow::Error> {
        if self.left == 0 {
            self.load().await?;
        }
        if self.left == 0 {
            return Err(anyhow!("Unexpected EoF"));
        }
        self.left -= 1;
        let res = unsafe { *self.next };
        self.next = self.next.wrapping_add(1);
        Ok(res)
    }

    async fn skip(&mut self, skip: usize) -> Result<(), anyhow::Error> {
        let mut skip = skip;
        while skip > self.left {
            skip -= self.left;
            self.left = 0;
            self.load().await?;
        }

        self.left -= skip;
        self.next = self.next.wrapping_add(skip);
        Ok(())
    }

    async fn zpull(&mut self, strm: &mut ZStreamWrap) -> Result<(), anyhow::Error> {
        if self.left == 0 {
            self.load().await?;
        }
        if self.left == 0 {
            return Err(anyhow!("Unexpected EoF"));
        }
        strm.stream.avail_in = self.left as u32;
        strm.stream.next_in = self.next;
        Ok(())
    }

    async fn get4(&mut self) -> Result<u32, anyhow::Error> {
        let mut res: u32 = self.get().await? as u32;
        res += (self.get().await? as u32) << 8;
        res += (self.get().await? as u32) << 16;
        res += (self.get().await? as u32) << 24;
        Ok(res)
    }

    async fn gzhead(&mut self) -> Result<usize, anyhow::Error> {
        if self.left == 0 {
            self.load().await?;
        }
        let left_before_header = self.left;

        /* verify gzip magic header and compression method */
        if self.get().await? != 0x1f || self.get().await? != 0x8b || self.get().await? != 8 {
            return Err(anyhow!("File is not a valid gzip archive"));
        }

        /* get and verify flags */
        let flags = self.get().await?;
        if flags & 0xe0 != 0 {
            return Err(anyhow!("Unknown reserved bits are set"));
        }

        self.skip(6).await?;

        /* skip extra field if present */
        if flags & 4 > 0 {
            let mut len = self.get().await? as usize;
            len += (self.get().await? as usize) << 8;
            self.skip(len).await?;
        }

        /* skip file name if present */
        if flags & 8 > 0 {
            while self.get().await? != 0 {}
        }

        /* skip comment if present */
        if flags & 16 > 0 {
            while self.get().await? != 0 {}
        }

        /* skip header crc if present */
        if flags & 2 > 0 {
            self.skip(2).await?;
        }

        self.skip = left_before_header - self.left;
        Ok(left_before_header - self.left)
    }

    async fn reset_and_pull(&mut self, strm: &mut ZStreamWrap) -> Result<(), anyhow::Error> {
        self.left = 0;
        self.skip = 0;
        self.zpull(strm).await?;
        Ok(())
    }

    fn copy_processed_buffer(&mut self, strm: &mut ZStreamWrap, include_next_byte: bool) -> Result<Vec<u8>, anyhow::Error> {
        let mut offset = unsafe { strm.stream.next_in.offset_from(self.buf.as_ptr()) };
        if !include_next_byte {
            offset -= 1;
        }
        let start = self.skip;
        self.skip = offset.try_into()?;
        Ok(self.buf[start..offset.try_into()?].to_vec())
    }

    fn next(&self) -> *mut u8 {
        self.next
    }

    fn set_read_ptr(&mut self, next: *mut u8, left: usize) {
        self.left = left;
        self.next = next;
    }
}

unsafe impl<S: Stream<Item = Vec<u8>> + Unpin> Send for StreamIn<S> {}

struct ZStreamWrap {
    pub stream: z_stream,
    pub junk: Box<[u8; CHUNK]>,
}

impl ZStreamWrap {
    pub fn new() -> Self{
        let stream = z_stream {
            next_in: std::ptr::null_mut(),
            avail_in: 0,
            total_in: 0,
            next_out: std::ptr::null_mut(),
            avail_out: 0,
            total_out: 0,
            msg: std::ptr::null_mut(),
            adler: 0,
            data_type: 0,
            reserved: 0,
            opaque: std::ptr::null_mut(),
            state: std::ptr::null_mut(),
            zalloc: allocator::zalloc,
            zfree: allocator::zfree,
        };
        ZStreamWrap {
            stream,
            junk: vec![0u8; CHUNK].try_into().unwrap(),
        }
    }

    pub fn init(&mut self) -> Result<(), anyhow::Error> {
        let ret = unsafe { inflateInit2_(&mut self.stream, -15, std::ptr::null(), 0) };
        if ret != Z_OK {
            return Err(anyhow!("Failed to init inflate machinery"));
        }
        self.stream.avail_out = 0;
        Ok(())
    }

    pub fn decompress_availble_data(&mut self) -> Result<usize, anyhow::Error> {
        self.stream.avail_out = CHUNK as u32;
        self.stream.next_out = self.junk.as_mut_ptr();
        let ret = unsafe { inflate(&mut self.stream, Z_BLOCK) };

        if ret == Z_MEM_ERROR {
            return Err(anyhow!("Out of memory"));
        }
        if ret == Z_DATA_ERROR {
            return Err(anyhow!("invalid compressed data"));
        }

        Ok(CHUNK - self.stream.avail_out as usize)
    }

    pub fn can_read_more(&self) -> bool {
        self.stream.avail_in > 0
    }

    pub fn can_write_more(&self) -> bool {
        self.stream.avail_out > 0
    }

    pub fn is_block_boundary(&self) -> bool {
        self.stream.data_type & 128 > 0
    }

    pub fn clear_last_flag(&self) -> bool {
        let last = unsafe { *self.stream.next_in.wrapping_add(0) } & 1 > 0;
        if last {
            unsafe { *self.stream.next_in.wrapping_add(0) &= !1 };
        }
        last
    }

    pub fn clear_last_flag_with_unused_bits(&self, pos: u8) -> bool {
        let pos = (0x100u16 >> pos) as u8;
        let last = unsafe { *self.stream.next_in.wrapping_sub(1) } & pos > 0; //???????
        if last {
            unsafe {
                *self.stream.next_in.wrapping_sub(1) &= !pos;
            }
        };
        last
    }

    pub fn get_unused_bits(&self) -> u8 {
        (self.stream.data_type & 7) as u8
    }

    pub fn get_last_byte(&self) -> u8 {
        unsafe { *self.stream.next_in.wrapping_sub(1) }
    }

    pub fn cleanup(&mut self) {
        unsafe { inflateEnd(&mut self.stream) };
    }
}

unsafe impl Send for ZStreamWrap {}

pub fn join_gzip<S: Stream<Item = Vec<u8>>>(
    data: S,
) -> impl Stream<Item = Result<Bytes, anyhow::Error>> {
    stream! {
        let mut crc = unsafe { libz_ng_sys::crc32(0, std::ptr::null(), 0) };
        let mut tot = 0u32;
        let buf = hex!("1f8b08000000000000ff");
        yield Ok(buf.to_vec().into());
        pin_mut!(data);
        let mut input = StreamIn::new(data);
        // let mut junk: Box<[u8; CHUNK]> = vec![0u8; CHUNK].try_into().unwrap();

        let mut strm_wrap = ZStreamWrap::new();
        loop {
            let _ = match input.gzhead().await {
                Ok(res) => { res },
                Err(_) => {
                    yield Ok([&[3u8, 0u8], &crc.to_le_bytes()[..], &tot.to_le_bytes()[..]].concat().into());
                    break;
                },
            };

            strm_wrap.init()?;

            /* inflate and copy compressed data, clear last-block bit if requested */
            let mut len = 0;
            input.zpull(&mut strm_wrap).await?;
            let mut last = strm_wrap.clear_last_flag();
            loop {
                /* if input used and output done, write used input and get more */
                if !strm_wrap.can_read_more() && strm_wrap.can_write_more() {
                    let buffer = input.copy_processed_buffer(&mut strm_wrap, true)?;
                    yield Ok(buffer.into());
                    input.reset_and_pull(&mut strm_wrap).await?;
                }
                /* decompress -- return early when end-of-block reached */
                /* update length of uncompressed data */
                len += strm_wrap.decompress_availble_data()?;

                /* check for block boundary (only get this when block copied out) */
                if strm_wrap.is_block_boundary() {
                    /* if that was the last block, then done */
                    if last {
                        break;
                    }
                    /* number of unused bits in last byte */
                    let pos = strm_wrap.get_unused_bits();
                    /* find the next last-block bit */
                    if pos != 0 {
                        /* next last-block bit is in last used byte */
                        last = strm_wrap.clear_last_flag_with_unused_bits(pos);
                    } else {
                        /* next last-block bit is in next unused byte */
                        if !strm_wrap.can_read_more() {
                            /* don't have that byte yet -- get it */
                            let buffer = input.copy_processed_buffer(&mut strm_wrap, true)?;
                            yield Ok(buffer.into());
                            input.reset_and_pull(&mut strm_wrap).await?
                        }
                        last = strm_wrap.clear_last_flag();
                    }
                } else {
                    if strm_wrap.can_read_more() {
                        let buffer = input.copy_processed_buffer(&mut strm_wrap, false)?;
                        yield Ok(buffer.into());
                    }
                }
            }

            /* update buffer with unused input */
            input.set_read_ptr(strm_wrap.stream.next_in, strm_wrap.stream.avail_in as usize);
            /* copy used input, write empty blocks to get to byte boundary */
            let pos = strm_wrap.get_unused_bits();
            let buffer = input.copy_processed_buffer(&mut strm_wrap, false)?;
            yield Ok(buffer.into());

            let mut last_byte = strm_wrap.get_last_byte(); //unsafe { *input.next().wrapping_sub(1) };
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
            strm_wrap.cleanup();
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::RngCore;
    use tokio::io::AsyncBufReadExt;

    use super::*;

    fn generate_data(data: Vec<Vec<u8>>) -> impl Stream<Item = Vec<u8>> {
        stream! {
            for datum in data {
                let encoder = GzipEncoder::with_quality(&datum[..], async_compression::Level::Fastest);
                let buf = BufReader::with_capacity(100_000_000, encoder).fill_buf().await.unwrap().to_vec();
                yield buf;
            }
        }
    }

    async fn pack_join_unpack(testcase: Vec<Vec<u8>>) {
        let gt = testcase.concat();
        let stream = join_gzip(generate_data(testcase));
        pin_mut!(stream);
        let reader = StreamReader::new(
            stream.map(|result| std::io::Result::Ok(Bytes::from_owner(result.unwrap()))),
        );
        let decoder = GzipDecoder::new(reader);
        let mut buf_reader = BufReader::with_capacity(100_000_000, decoder);
        let result = buf_reader.fill_buf().await.unwrap();
        assert_eq!(result.to_vec(), gt);
    }

    #[tokio::test]
    async fn test_smoke() {
        let input = vec![vec![1u8, 2], vec![3, 4]];
        pack_join_unpack(input).await;
    }

    #[tokio::test]
    async fn test_const() {
        let input = vec![vec![1u8; 100_000], vec![3; 100_000]];
        pack_join_unpack(input).await;
    }

    #[tokio::test]
    async fn test_rand() {
        let mut rng = rand::rng();
        let input = (0..99)
            .map(|_| {
                let mut data = [0u8; 100_000];
                rng.fill_bytes(&mut data);
                data.to_vec()
            })
            .collect();
        pack_join_unpack(input).await;
    }

    #[tokio::test]
    async fn test_rand_long() {
        let mut rng = rand::rng();
        let input = (0..9)
            .map(|_| {
                let mut data = [0u8; 1_000_000];
                rng.fill_bytes(&mut data);
                data.to_vec()
            })
            .collect();
        pack_join_unpack(input).await;
    }
}

use std::{slice, sync::{Arc, Mutex}};

use anyhow::anyhow;
use hex_literal::hex;
use async_compression::tokio::bufread::{GzipDecoder, GzipEncoder};
use async_stream::stream;
use futures::{Stream, StreamExt, pin_mut};
use itertools::Itertools;
use libz_ng_sys::{Z_BLOCK, Z_DATA_ERROR, Z_MEM_ERROR, Z_OK, crc32_combine, inflate, inflateEnd, inflateInit2_, z_stream};
use tokio::io::BufReader;
use tokio_util::{
    bytes::Bytes,
    io::{ReaderStream, StreamReader},
};

const CHUNK: usize = 32768;

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

// TODO: implement fast concatenation algorithm because this is currently the bottleneck in streaming
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
}

impl <S: Stream<Item = Vec<u8>> + Unpin> StreamIn<S> {
    fn new(stream: S) -> Self {
        Self {
            stream,
            left: 0,
            next: std::ptr::null_mut(),
            buf: Default::default(),
        }
    }

    async fn load(&mut self) -> Result<(), anyhow::Error> {
        self.buf = self.stream.next().await.ok_or(anyhow!("faild to load data from incoming stream"))?;
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

    async fn zpull(&mut self, strm: &mut zstream_wrap) -> Result<(), anyhow::Error> {
        if self.left == 0 {
            self.load().await?;
        }
        if self.left == 0 {
            return Err(anyhow!("Unexpected EoF"));
        }
        strm.0.avail_in = self.left as u32;
        strm.0.next_in = self.next;
        Ok(())
    }

    async fn get4(&mut self) -> Result<u32, anyhow::Error> {
        let mut res: u32 = self.get().await? as u32;
        res += (self.get().await? as u32) << 8;
        res += (self.get().await? as u32) << 16;
        res += (self.get().await? as u32) << 24;
        Ok(res)
    }

    async fn gzhead(&mut self) -> Result<(), anyhow::Error> {
        if self.left == 0 {
            self.load().await?;
        }
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

        Ok(())
    }

    async fn reset_and_pull(&mut self,  strm: &mut zstream_wrap) -> Result<(), anyhow::Error> {
        self.left = 0;
        self.zpull(strm).await?;
        //Ok(self.buf.as_mut_ptr())
        Ok(())
    }

    fn next(&self) -> *mut u8 {
        self.next
    }

    fn set_read_ptr(&mut self, next: *mut u8, left: usize) {
        self.left = left;
        self.next = next;
    }
}


unsafe impl <S: Stream<Item = Vec<u8>> + Unpin> Send for StreamIn<S> {
    
}

struct zstream_wrap(z_stream);

unsafe impl Send for zstream_wrap {
    
}

pub fn join_gzip<S: Stream<Item = Vec<u8>>>(data: S)
    -> impl Stream<Item = Result<Bytes, anyhow::Error>>
{
    
    stream! {
        
        let mut crc = unsafe { libz_ng_sys::crc32(0, std::ptr::null(), 0) };
        let mut tot = 0u32;
        let buf = hex!("1f8b08000000000000ff");
        yield Ok(buf.to_vec().into());
        pin_mut!(data);
        let mut input = StreamIn::new(data);
        let mut junk: Box<[u8; CHUNK]> = vec![0u8; CHUNK].try_into().unwrap();

        let mut strm_wrap = zstream_wrap(z_stream {
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
        });
        loop {
            // let mut strm = strm_mutex.lock().unwrap();

            match input.gzhead().await {
                Ok(_) => {},
                Err(_) => {
                    yield Ok([&[3u8, 0u8], &crc.to_le_bytes()[..], &tot.to_le_bytes()[..]].concat().into());
                    break;
                },
            };


            let mut ret = unsafe { inflateInit2_(&mut strm_wrap.0, -15, std::ptr::null(), 0) };
            if ret != Z_OK {
                yield Err(anyhow!("Failed to init inflate machinery"));
                break;
            }

            /* inflate and copy compressed data, clear last-block bit if requested */
            let mut len = 0;
            input.zpull(&mut strm_wrap).await?;
            //let mut start = strm_wrap.0.next_in;
            let mut last = unsafe { *strm_wrap.0.next_in.wrapping_add(0) } & 1 > 0;
            if last {
                unsafe { *strm_wrap.0.next_in.wrapping_add(0) &= !1 };
            }
            strm_wrap.0.avail_out = 0;
            loop {
                /* if input used and output done, write used input and get more */
                if strm_wrap.0.avail_in == 0 && strm_wrap.0.avail_out != 0 {
                    let buffer: &[u8] = unsafe {
                        let start = input.buf.as_mut_ptr();
                        slice::from_raw_parts(start, strm_wrap.0.next_in.offset_from(start).try_into()?)
                    };
                    yield Ok(buffer.to_vec().into());
                    //start = input.reset_and_pull(&mut strm_wrap).await?;
                    input.reset_and_pull(&mut strm_wrap).await?;
                }
                /* decompress -- return early when end-of-block reached */
                strm_wrap.0.avail_out = CHUNK as u32;
                strm_wrap.0.next_out = junk.as_mut_ptr();
                ret = unsafe { inflate(&mut strm_wrap.0, Z_BLOCK) };

                if ret == Z_MEM_ERROR {
                    yield Err(anyhow!("Out of memory"));
                    return;
                }
                if ret == Z_DATA_ERROR {
                    yield Err(anyhow!("invalid compressed data"));
                    return;
                }

                /* update length of uncompressed data */
                len += CHUNK - strm_wrap.0.avail_out as usize;
                /* check for block boundary (only get this when block copied out) */
                if strm_wrap.0.data_type & 128 > 0 {
                    /* if that was the last block, then done */
                    if last {
                        break;
                    }
                    /* number of unused bits in last byte */
                    let mut pos = (strm_wrap.0.data_type & 7) as u8;
                    /* find the next last-block bit */
                    if pos != 0 {
                        /* next last-block bit is in last used byte */
                        pos = (0x100u16 >> pos) as u8;
                        last = unsafe { *strm_wrap.0.next_in.wrapping_sub(1) } & pos > 0; //???????
                        if last {
                            unsafe {
                                *strm_wrap.0.next_in.wrapping_sub(1) &= !pos;
                            }
                        }
                    } else {
                        /* next last-block bit is in next unused byte */
                        if strm_wrap.0.avail_in == 0 {
                            /* don't have that byte yet -- get it */
                            let buffer: &[u8] = unsafe {
                                let start = input.buf.as_mut_ptr();
                                slice::from_raw_parts(start, strm_wrap.0.next_in.offset_from(start).try_into()?)
                            };
                            yield Ok(buffer.to_vec().into());
                            input.reset_and_pull(&mut strm_wrap).await?
                        }
                        last = unsafe { *strm_wrap.0.next_in.wrapping_add(0) } & 1 > 0;
                        if last {
                            unsafe { *strm_wrap.0.next_in.wrapping_add(0) &= !1 };
                        }
                    }
                }
            }

            /* update buffer with unused input */
            input.set_read_ptr(strm_wrap.0.next_in, strm_wrap.0.avail_in as usize);
            /* copy used input, write empty blocks to get to byte boundary */
            let pos = strm_wrap.0.data_type & 7;
            let buffer: &[u8] = unsafe {
                let start = input.buf.as_mut_ptr();
                slice::from_raw_parts(start, (input.next().offset_from(start) - 1).try_into()?)
            };
            yield Ok(buffer.to_vec().into());

            let mut last_byte = unsafe { *input.next().wrapping_sub(1) };
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
            unsafe { inflateEnd(&mut strm_wrap.0) };
        }
    }
}

use anyhow::anyhow;
use futures::Stream;
use libz_ng_sys::{Z_BLOCK, Z_DATA_ERROR, Z_MEM_ERROR, Z_OK, inflate, inflateEnd, inflateInit2_, z_stream};
use tokio_stream::StreamExt;

// const CHUNK: usize = 32768;
const CHUNK: usize = 128;



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

pub struct GzStreamHolder<S: Stream<Item = Vec<u8>> + Unpin> {
    stream: S,
    left: usize,
    next: usize,
    buf: Vec<u8>,
    skip: usize,
    zstream: Box::<z_stream>,
    junk: Box<[u8; CHUNK]>,
}

impl<S: Stream<Item = Vec<u8>> + Unpin> GzStreamHolder<S> {
    pub fn new(stream: S) -> Self {
        let zstream = Box::new(z_stream {
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

        Self {
            stream,
            left: 0,
            next: 0,
            buf: Default::default(),
            skip: 0,
            zstream,
            junk: vec![0u8; CHUNK].try_into().unwrap(),
        }
    }

    async fn load(&mut self) -> Result<(), anyhow::Error> {
        self.buf = self
            .stream
            .next()
            .await
            .ok_or(anyhow!("faild to load data from incoming stream"))?;
        self.left = self.buf.len();
        self.next = 0;
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
        let res = self.buf[self.next];
        self.next += 1;
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
        self.next += skip;
        Ok(())
    }

    pub async fn get4(&mut self) -> Result<u32, anyhow::Error> {
        let mut res: u32 = self.get().await? as u32;
        res += (self.get().await? as u32) << 8;
        res += (self.get().await? as u32) << 16;
        res += (self.get().await? as u32) << 24;
        Ok(res)
    }

    pub async fn gzhead(&mut self) -> Result<usize, anyhow::Error> {
        if self.left == 0 {
            self.load().await?;
        }
        // let left_before_header = self.left;

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
        if self.left > 0 {
            self.skip = self.buf.len() - self.left;
        } else {
            self.skip = 0;
        }

        //Ok(left_before_header - self.left)
        Ok(self.skip)
    }

    pub async fn zpull(&mut self) -> Result<(), anyhow::Error> {
        if self.left == 0 {
            self.load().await?;
        }
        if self.left == 0 {
            return Err(anyhow!("Unexpected EoF"));
        }
        self.zstream.avail_in = self.left as u32;
        self.zstream.next_in = self.buf.as_mut_ptr().wrapping_add(self.next); //self.next;
        Ok(())
    }

    pub async fn reset_and_pull(&mut self) -> Result<(), anyhow::Error> {
        self.left = 0;
        self.skip = 0;
        self.zpull().await?;
        Ok(())
    }

    pub fn copy_processed_buffer(&mut self, include_next_byte: bool) -> Result<Vec<u8>, anyhow::Error> {
        let mut offset = unsafe { self.zstream.next_in.offset_from(self.buf.as_ptr()) };
        if !include_next_byte {
            offset -= 1;
        }
        let start = self.skip;
        self.skip = offset.try_into()?;
        Ok(self.buf[start..offset.try_into()?].to_vec())
    }

    pub fn set_read_ptr(&mut self) {
        self.left = self.zstream.avail_in as usize;
        self.next = self.buf.len() - self.left; //strm.stream.next_in;
    }

    pub fn init(&mut self) -> Result<(), anyhow::Error> {
        let ret = unsafe { inflateInit2_(&mut *self.zstream, -15, std::ptr::null(), 0) };
        if ret != Z_OK {
            return Err(anyhow!("Failed to init inflate machinery"));
        }
        self.zstream.avail_out = 0;
        Ok(())
    }

    pub fn decompress_availble_data(&mut self) -> Result<usize, anyhow::Error> {
        self.zstream.avail_out = CHUNK as u32;
        self.zstream.next_out = self.junk.as_mut_ptr();
        let ret = unsafe { inflate(&mut *self.zstream, Z_BLOCK) };

        if ret == Z_MEM_ERROR {
            return Err(anyhow!("Out of memory"));
        }
        if ret == Z_DATA_ERROR {
            return Err(anyhow!("invalid compressed data"));
        }

        Ok(CHUNK - self.zstream.avail_out as usize)
    }

    pub fn can_read_more(&self) -> bool {
        self.zstream.avail_in > 0
    }

    pub fn can_write_more(&self) -> bool {
        self.zstream.avail_out > 0
    }

    pub fn is_block_boundary(&self) -> bool {
        self.zstream.data_type & 128 > 0
    }

    pub fn clear_last_flag(&self) -> bool {
        let last = unsafe { *self.zstream.next_in.wrapping_add(0) } & 1 > 0;
        if last {
            unsafe { *self.zstream.next_in.wrapping_add(0) &= !1; };
        }
        last
    }

    pub fn clear_last_flag_with_unused_bits(&self, pos: u8) -> bool {
        let pos = (0x100u16 >> pos) as u8;
        let last = unsafe { *self.zstream.next_in.wrapping_sub(1) } & pos > 0;
        if last {
            unsafe { *self.zstream.next_in.wrapping_sub(1) &= !pos; };
        };
        last
    }

    pub fn get_unused_bits(&self) -> u8 {
        (self.zstream.data_type & 7) as u8
    }

    pub fn get_last_byte(&self) -> u8 {
        unsafe { *self.zstream.next_in.wrapping_sub(1) }
    }

    pub fn cleanup(&mut self) {
        unsafe { inflateEnd(&mut *self.zstream) };
    }
}

unsafe impl<S: Stream<Item = Vec<u8>> + Unpin> Send for GzStreamHolder<S> {}

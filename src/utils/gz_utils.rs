use anyhow::anyhow;
use futures::Stream;
use libz_ng_sys::{
    inflate, inflateEnd, inflateInit2_, z_stream, Z_BLOCK, Z_DATA_ERROR, Z_MEM_ERROR, Z_OK,
};
use tokio_stream::StreamExt;

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
    zstream: Box<z_stream>,
    chunk_size: usize,
    junk: Box<[u8]>,
}

impl<S: Stream<Item = Vec<u8>> + Unpin> GzStreamHolder<S> {
    pub fn new(stream: S, chunk_size: usize) -> Self {
        assert!(chunk_size > 0, "chunk size must be positive");
        assert!(
            chunk_size <= u32::MAX as usize,
            "chunk size must fit into u32 for zlib"
        );
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
            chunk_size,
            junk: vec![0u8; chunk_size].into_boxed_slice(),
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

    pub fn copy_processed_buffer(
        &mut self,
        include_next_byte: bool,
    ) -> Result<Vec<u8>, anyhow::Error> {
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
        self.zstream.avail_out = self.chunk_size as u32;
        self.zstream.next_out = self.junk.as_mut_ptr();
        let ret = unsafe { inflate(&mut *self.zstream, Z_BLOCK) };

        if ret == Z_MEM_ERROR {
            return Err(anyhow!("Out of memory"));
        }
        if ret == Z_DATA_ERROR {
            return Err(anyhow!("invalid compressed data"));
        }

        Ok(self.chunk_size - self.zstream.avail_out as usize)
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
            unsafe {
                *self.zstream.next_in.wrapping_add(0) &= !1;
            };
        }
        last
    }

    pub fn clear_last_flag_with_unused_bits(&self, pos: u8) -> bool {
        let pos = (0x100u16 >> pos) as u8;
        let last = unsafe { *self.zstream.next_in.wrapping_sub(1) } & pos > 0;
        if last {
            unsafe {
                *self.zstream.next_in.wrapping_sub(1) &= !pos;
            };
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

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::DeflateEncoder;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use futures::stream;
    use std::io::Write;

    type EmptyStream = stream::Iter<std::vec::IntoIter<Vec<u8>>>;

    const GZIP_HEADER_LEN: usize = 10;
    const STORED_BLOCK_OVERHEAD: usize = 5; // 1b header + LEN + NLEN
    const TEST_CHUNK_SIZE: usize = 128;
    const MULTI_BLOCK_SAMPLE_LEN: usize = 70_000;

    fn holder_with_buffer(buf: Vec<u8>, offset: usize) -> GzStreamHolder<EmptyStream> {
        let stream = stream::iter(Vec::<Vec<u8>>::new());
        let mut holder = GzStreamHolder::new(stream, TEST_CHUNK_SIZE);
        assert!(
            offset <= buf.len(),
            "offset must point inside or one past buffer"
        );
        holder.buf = buf;
        unsafe {
            holder.zstream.next_in = holder.buf.as_mut_ptr().add(offset);
        }
        holder
    }

    #[tokio::test]
    async fn test_gzhead_basic() {
        let data = b"hello world";
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).unwrap();
        let compressed = encoder.finish().unwrap();
        let stream = stream::iter(vec![compressed]);
        let mut holder = GzStreamHolder::new(stream, TEST_CHUNK_SIZE);
        let skip = holder.gzhead().await.unwrap();
        assert_eq!(skip, 10);
    }

    #[tokio::test]
    async fn test_gzhead_with_filename() {
        let data = b"hello";
        let mut deflate_encoder = DeflateEncoder::new(Vec::new(), Compression::default());
        deflate_encoder.write_all(data).unwrap();
        let deflated = deflate_encoder.finish().unwrap();
        let mut compressed = vec![0x1f, 0x8b, 0x08, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff];
        compressed.extend_from_slice(b"file");
        compressed.push(0);
        compressed.extend_from_slice(&deflated);
        let stream = stream::iter(vec![compressed]);
        let mut holder = GzStreamHolder::new(stream, TEST_CHUNK_SIZE);
        let skip = holder.gzhead().await.unwrap();
        assert_eq!(skip, 15);
    }

    #[tokio::test]
    async fn test_gzhead_invalid_magic() {
        let compressed = vec![0x1f, 0x8c, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff];
        let stream = stream::iter(vec![compressed]);
        let mut holder = GzStreamHolder::new(stream, TEST_CHUNK_SIZE);
        let res = holder.gzhead().await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_gzhead_reserved_flags() {
        let compressed = vec![0x1f, 0x8b, 0x08, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff];
        let stream = stream::iter(vec![compressed]);
        let mut holder = GzStreamHolder::new(stream, TEST_CHUNK_SIZE);
        let res = holder.gzhead().await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_decompress_length() {
        let data = b"hello world";
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).unwrap();
        let compressed = encoder.finish().unwrap();
        let stream = stream::iter(vec![compressed]);
        let mut holder = GzStreamHolder::new(stream, TEST_CHUNK_SIZE);
        holder.gzhead().await.unwrap();
        holder.init().unwrap();
        holder.zpull().await.unwrap();
        let len = holder.decompress_availble_data().unwrap();
        assert_eq!(len, data.len());
    }

    #[tokio::test]
    async fn test_decompress_length_large() {
        let data = vec![b'a'; 200];
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&data).unwrap();
        let compressed = encoder.finish().unwrap();
        let stream = stream::iter(vec![compressed]);
        let mut holder = GzStreamHolder::new(stream, TEST_CHUNK_SIZE);
        holder.gzhead().await.unwrap();
        holder.init().unwrap();
        holder.zpull().await.unwrap();
        let len = holder.decompress_availble_data().unwrap();
        // Since data is 200, and TEST_CHUNK_SIZE=128, first decompress gives 128
        assert_eq!(len, 128);
    }

    #[test]
    fn test_clear_last_flag_clears_bfinal_bit() {
        let holder = holder_with_buffer(vec![0b0000_0001, 0u8], 0);
        let had_last_flag = holder.clear_last_flag();
        assert!(had_last_flag);
        assert_eq!(holder.buf[0] & 1, 0);

        // second call sees the bit already cleared and should report false
        assert!(!holder.clear_last_flag());
    }

    #[test]
    fn test_clear_last_flag_with_unused_bits_masks_previous_byte() {
        let holder = holder_with_buffer(vec![0b1001_0000, 0b0000_0000], 1);

        // pos=1 -> mask 0b1000_0000, clearing the highest bit in the previous byte
        assert!(holder.clear_last_flag_with_unused_bits(1));
        assert_eq!(holder.buf[0], 0b0001_0000);

        // pos=4 -> mask 0b0001_0000, clearing the lower nibble flag
        assert!(holder.clear_last_flag_with_unused_bits(4));
        assert_eq!(holder.buf[0], 0);

        // Once both bits are cleared, subsequent calls should return false
        assert!(!holder.clear_last_flag_with_unused_bits(4));
    }

    #[test]
    fn test_bit_introspection_helpers() {
        let mut holder = holder_with_buffer(vec![0xAA, 0xBC, 0xDE], 2);
        holder.zstream.data_type = 0b1010_0101;

        assert_eq!(holder.get_unused_bits(), 0b0000_0101);
        assert_eq!(holder.get_last_byte(), 0xBC);
    }

    fn stored_block_size(payload_len: usize) -> usize {
        assert!(payload_len <= u16::MAX as usize);
        STORED_BLOCK_OVERHEAD + payload_len
    }

    fn build_stored_block(data: &[u8], is_final: bool) -> Vec<u8> {
        assert!(data.len() <= u16::MAX as usize);
        let mut block = Vec::with_capacity(stored_block_size(data.len()));
        block.push(if is_final { 0x01 } else { 0x00 });
        let len = data.len() as u16;
        block.extend_from_slice(&len.to_le_bytes());
        block.extend_from_slice(&(!len).to_le_bytes());
        block.extend_from_slice(data);
        block
    }

    fn build_member_from_blocks(blocks: &[&[u8]]) -> Vec<u8> {
        assert!(!blocks.is_empty());
        let mut member = vec![0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff];
        let mut crc = unsafe { libz_ng_sys::crc32(0, std::ptr::null(), 0) };
        let mut total = 0u32;

        for (idx, block) in blocks.iter().enumerate() {
            let block_bytes = build_stored_block(block, idx == blocks.len() - 1);
            member.extend_from_slice(&block_bytes);
            total += block.len() as u32;
            unsafe {
                crc = libz_ng_sys::crc32(crc, block.as_ptr(), block.len() as u32);
            }
        }

        member.extend_from_slice(&crc.to_le_bytes());
        member.extend_from_slice(&total.to_le_bytes());
        member
    }

    fn crc32_for_blocks(blocks: &[&[u8]]) -> u32 {
        unsafe {
            let mut crc = libz_ng_sys::crc32(0, std::ptr::null(), 0);
            for block in blocks {
                crc = libz_ng_sys::crc32(crc, block.as_ptr(), block.len() as u32);
            }
            crc
        }
    }

    fn gzip_multiblock_member(len: usize) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::none());
        let payload: Vec<u8> = (0..len).map(|idx| (idx % 251) as u8).collect();
        encoder.write_all(&payload).unwrap();
        encoder.finish().unwrap()
    }

    #[tokio::test]
    async fn test_block_state_helpers_on_multi_block_members() {
        let block_alpha: &[u8] = b"alpha-block-payload";
        let block_beta: &[u8] = b"beta-block-data-that-continues";
        let block_gamma: &[u8] = b"gamma-member-ending";

        let first_member_blocks = [block_alpha, block_beta];
        let second_member_blocks = [block_gamma];
        let member_one = build_member_from_blocks(&first_member_blocks);
        let member_two = build_member_from_blocks(&second_member_blocks);
        let combined: Vec<u8> = member_one
            .iter()
            .chain(member_two.iter())
            .copied()
            .collect();

        let first_chunk_len = GZIP_HEADER_LEN + stored_block_size(block_alpha.len());
        let (chunk_a, chunk_b) = combined.split_at(first_chunk_len);
        let stream = stream::iter(vec![chunk_a.to_vec(), chunk_b.to_vec()]);
        let mut holder = GzStreamHolder::new(stream, TEST_CHUNK_SIZE);

        holder.gzhead().await.unwrap();
        holder.init().unwrap();
        holder.zpull().await.unwrap();

        let produced_alpha = holder.decompress_availble_data().unwrap();
        assert_eq!(produced_alpha, block_alpha.len());
        assert!(holder.is_block_boundary());
        assert_eq!(holder.get_unused_bits(), 0);
        assert!(holder.can_write_more());
        assert!(!holder.can_read_more());

        holder.reset_and_pull().await.unwrap();

        let produced_beta = holder.decompress_availble_data().unwrap();
        assert_eq!(produced_beta, block_beta.len());
        assert!(holder.is_block_boundary());
        assert_eq!(holder.get_unused_bits(), 0);
        assert!(holder.can_write_more());
        assert!(holder.can_read_more());

        holder.set_read_ptr();
        let crc_first = holder.get4().await.unwrap();
        let len_first = holder.get4().await.unwrap();
        assert_eq!(crc_first, crc32_for_blocks(&first_member_blocks));
        assert_eq!(len_first as usize, block_alpha.len() + block_beta.len());
        holder.cleanup();

        holder.gzhead().await.unwrap();
        holder.init().unwrap();
        holder.zpull().await.unwrap();

        let produced_gamma = holder.decompress_availble_data().unwrap();
        assert_eq!(produced_gamma, block_gamma.len());
        assert!(holder.is_block_boundary());
        assert_eq!(holder.get_unused_bits(), 0);
        assert!(holder.can_write_more());
        assert!(holder.can_read_more());

        holder.set_read_ptr();
        let crc_second = holder.get4().await.unwrap();
        let len_second = holder.get4().await.unwrap();
        assert_eq!(crc_second, crc32_for_blocks(&second_member_blocks));
        assert_eq!(len_second as usize, block_gamma.len());
        holder.cleanup();
    }

    #[tokio::test]
    async fn test_copy_processed_buffer_excludes_next_byte_at_block_boundary() {
        let compressed = gzip_multiblock_member(MULTI_BLOCK_SAMPLE_LEN);
        let stream = stream::iter(vec![compressed.clone()]);
        let mut holder = GzStreamHolder::new(stream, TEST_CHUNK_SIZE);

        holder.gzhead().await.unwrap();
        holder.init().unwrap();
        holder.zpull().await.unwrap();
        let _ = holder.clear_last_flag();

        let mut guard = 0usize;
        loop {
            holder.decompress_availble_data().unwrap();
            guard += 1;
            assert!(guard < 1024, "inflate failed to reach block boundary");
            if holder.is_block_boundary() {
                break;
            }
        }

        assert!(holder.can_read_more(), "expected more than one gzip block");

        let start = holder.skip;
        let next_in_offset =
            unsafe { holder.zstream.next_in.offset_from(holder.buf.as_ptr()) } as usize;
        assert!(
            next_in_offset > start + 1,
            "inflate consumed too few bytes for assertion"
        );

        let expected = holder.buf[start..next_in_offset - 1].to_vec();
        let extracted = holder.copy_processed_buffer(false).unwrap();
        assert_eq!(extracted, expected);
        assert_eq!(holder.skip, next_in_offset - 1);

        // without additional progress nothing new should be returned
        assert!(holder.copy_processed_buffer(false).unwrap().is_empty());

        holder.cleanup();
    }

    #[tokio::test]
    async fn test_copy_processed_buffer_includes_next_byte_when_fragment_is_exhausted() {
        let compressed = gzip_multiblock_member(MULTI_BLOCK_SAMPLE_LEN);
        let split_at = GZIP_HEADER_LEN + 256;
        assert!(split_at < compressed.len() - 8);
        let first = compressed[..split_at].to_vec();
        let second = compressed[split_at..].to_vec();
        let stream = stream::iter(vec![first.clone(), second]);
        let mut holder = GzStreamHolder::new(stream, TEST_CHUNK_SIZE);

        holder.gzhead().await.unwrap();
        holder.init().unwrap();
        holder.zpull().await.unwrap();
        let _ = holder.clear_last_flag();

        let mut guard = 0usize;
        loop {
            holder.decompress_availble_data().unwrap();
            guard += 1;
            assert!(guard < 1024, "inflate failed to exhaust first fragment");
            if !holder.can_read_more() && holder.can_write_more() {
                break;
            }
        }

        let start = holder.skip;
        let next_in_offset =
            unsafe { holder.zstream.next_in.offset_from(holder.buf.as_ptr()) } as usize;
        assert_eq!(next_in_offset, first.len());
        let expected = holder.buf[start..next_in_offset].to_vec();

        let extracted = holder.copy_processed_buffer(true).unwrap();
        assert!(!extracted.is_empty());
        assert_eq!(extracted, expected);
        assert_eq!(holder.skip, next_in_offset);

        holder.reset_and_pull().await.unwrap();
        assert!(holder.left > 0);
        assert_eq!(holder.skip, 0);

        holder.cleanup();
    }
}

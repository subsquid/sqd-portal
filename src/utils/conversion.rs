use async_compression::tokio::bufread::{GzipDecoder, GzipEncoder};
use futures::StreamExt;
use itertools::Itertools;
use tokio::io::BufReader;
use tokio_util::{
    bytes::Bytes,
    io::{ReaderStream, StreamReader},
};

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

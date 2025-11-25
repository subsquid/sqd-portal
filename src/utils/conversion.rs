use async_compression::tokio::bufread::{GzipDecoder, GzipEncoder};
use futures::StreamExt;
use itertools::Itertools;
use tokio::io::{AsyncReadExt, BufReader};
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

pub fn recompress_gzip<S>(stream: S) -> impl futures::Stream<Item = std::io::Result<Bytes>>
where
    S: futures::Stream<Item = Vec<u8>>,
{
    let reader =
        StreamReader::new(stream.map(|result| std::io::Result::Ok(Bytes::from_owner(result))));

    let mut decoder = GzipDecoder::new(reader);
    decoder.multiple_members(true);

    let encoder =
        GzipEncoder::with_quality(BufReader::new(decoder), async_compression::Level::Default);

    ReaderStream::new(encoder)
}

pub fn get_gzip_decoder<S>(
    stream: S,
) -> GzipDecoder<BufReader<tokio_util::io::StreamReader<S, bytes::Bytes>>>
where
    S: futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Unpin,
{
    let mut decoder = GzipDecoder::new(BufReader::new(StreamReader::new(stream)));
    decoder.multiple_members(true);
    decoder
}

pub async fn stream_to_string<S>(stream: S) -> anyhow::Result<String>
where
    S: futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Unpin,
{
    let mut decoder = get_gzip_decoder(stream);
    let mut bytes: Vec<u8> = Vec::new();
    decoder.read_to_end(&mut bytes).await?;
    Ok(String::from_utf8(bytes)?)
}

use std::io::{Read, Write};

use byteorder::{ByteOrder, ReadBytesExt, WriteBytesExt};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Get the size, in bytes, of the minimum number of bytes necessary to represent the specified
/// buffer size.
pub(crate) fn buffer_size_len(mut buf_size: u64) -> u8 {
    let mut res = 0;

    while buf_size > 0 {
        buf_size >>= 8;
        res += 1;
    }

    res
}

/// Write a buffer size len only.
pub(crate) fn write_buffer_size_len(
    mut w: impl Write,
    size_len: u8,
    info_bits: u8,
) -> std::io::Result<usize> {
    assert!(size_len <= 0x3f, "size_len should be stricly less than 64");
    assert!(info_bits <= 4, "info_bits should be stricly less than 4");

    let size_len = size_len | info_bits << 6;

    w.write_u8(size_len)?;

    Ok(1)
}

/// Write a buffer size len only.
pub(crate) async fn async_write_buffer_size_len(
    mut w: impl AsyncWrite + Unpin,
    size_len: u8,
    info_bits: u8,
) -> std::io::Result<usize> {
    assert!(size_len <= 0x3f, "size_len should be stricly less than 64");
    assert!(info_bits <= 4, "info_bits should be stricly less than 4");

    let size_len = size_len | info_bits << 6;

    w.write_all(&[size_len]).await?;

    Ok(1)
}

/// Write a buffer size to the specified `Write`.
///
/// The buffer size is written using the lowest possibly number of bytes, using the
/// least-significant bits to encode the buffer size length.
///
/// The `info_bits` is a 2 bits-max value that can be used to convey additional information into
/// the resulting value.
///
/// If the buffer size to write is 0, the write is optimized and contains only the `info_bits`.
///
/// Returns the number of bytes written.
pub(crate) fn write_buffer_size(
    mut w: impl Write,
    buf_size: u64,
    info_bits: u8,
) -> std::io::Result<usize> {
    let size_len = buffer_size_len(buf_size);
    write_buffer_size_len(&mut w, size_len, info_bits)?;

    if size_len > 0 {
        // The size should never take more than 8 bytes, ensuring that the buffer size never exceeds 64
        // bits.
        assert!(size_len <= 8, "size_len is too large ({size_len} > 8)");

        let mut size_buf = [0; 8];
        byteorder::NetworkEndian::write_u64(&mut size_buf, buf_size);

        let idx = size_buf.len() - size_len as usize;

        w.write_all(&size_buf[idx..])?;
    }

    Ok(size_len as usize + 1)
}

/// Write a buffer size to the specified `Write`.
///
/// The buffer size is written using the lowest possibly number of bytes, using the
/// least-significant bits to encode the buffer size length.
///
/// The `info_bits` is a 2 bits-max value that can be used to convey additional information into
/// the resulting value.
///
/// If the buffer size to write is 0, the write is optimized and contains only the `info_bits`.
///
/// Returns the number of bytes written.
pub(crate) async fn async_write_buffer_size(
    mut w: impl AsyncWrite + Unpin,
    buf_size: u64,
    info_bits: u8,
) -> std::io::Result<usize> {
    let size_len = buffer_size_len(buf_size);
    async_write_buffer_size_len(&mut w, size_len, info_bits).await?;

    if size_len > 0 {
        // The size should never take more than 8 bytes, ensuring that the buffer size never exceeds 64
        // bits.
        assert!(size_len <= 8, "size_len is too large ({size_len} > 8)");

        let mut size_buf = [0; 8];
        byteorder::NetworkEndian::write_u64(&mut size_buf, buf_size);

        let idx = size_buf.len() - size_len as usize;

        w.write_all(&size_buf[idx..]).await?;
    }

    Ok(size_len as usize + 1)
}

/// Read a buffer size len from the specified `Read`.
///
/// The second element of the returned tuple are the info bits.
pub(crate) fn read_buffer_size_len(mut r: impl Read) -> std::io::Result<(u8, u8)> {
    let size_len = r.read_u8()?;

    Ok((size_len & 0x3f, size_len >> 6))
}

/// Read a buffer size len from the specified `Read`.
///
/// The second element of the returned tuple are the info bits.
pub(crate) async fn async_read_buffer_size_len(
    mut r: impl AsyncRead + Unpin,
) -> std::io::Result<(u8, u8)> {
    let mut buf = vec![0x00; 1];
    r.read_exact(&mut buf).await?;
    let size_len = buf[0];

    Ok((size_len & 0x3f, size_len >> 6))
}

/// Read a buffer size from the specified `Read` using the specified size length.
///
/// The second element of the returned tuple are the info bits.
pub(crate) fn read_buffer_size_with_size_len(
    mut r: impl Read,
    size_len: u8,
) -> std::io::Result<u64> {
    if size_len == 0 {
        return Ok(0);
    }

    let mut size_buf = [0; 8];

    assert!(
        size_len as usize <= size_buf.len(),
        "size_len should never exceed {}",
        size_buf.len()
    );

    r.read_exact(&mut size_buf[8 - size_len as usize..])?;
    let size = byteorder::NetworkEndian::read_u64(&size_buf);

    Ok(size)
}

/// Read a buffer size from the specified `Read` using the specified size length.
///
/// The second element of the returned tuple are the info bits.
pub(crate) async fn async_read_buffer_size_with_size_len(
    mut r: impl AsyncRead + Unpin,
    size_len: u8,
) -> std::io::Result<u64> {
    if size_len == 0 {
        return Ok(0);
    }

    let mut size_buf = [0; 8];

    assert!(
        size_len as usize <= size_buf.len(),
        "size_len should never exceed {}",
        size_buf.len()
    );

    r.read_exact(&mut size_buf[8 - size_len as usize..]).await?;
    let size = byteorder::NetworkEndian::read_u64(&size_buf);

    Ok(size)
}

/// Read a buffer size from the specified `Read`.
///
/// The second element of the returned tuple are the info bits.
pub(crate) fn read_buffer_size(mut r: impl Read) -> std::io::Result<(u64, u8)> {
    let (size_len, info_bits) = read_buffer_size_len(&mut r)?;

    read_buffer_size_with_size_len(r, size_len).map(|buf_size| (buf_size, info_bits))
}

/// Read a buffer size from the specified `Read`.
///
/// The second element of the returned tuple are the info bits.
pub(crate) async fn async_read_buffer_size(
    mut r: impl AsyncRead + Unpin,
) -> std::io::Result<(u64, u8)> {
    let (size_len, info_bits) = async_read_buffer_size_len(&mut r).await?;

    async_read_buffer_size_with_size_len(r, size_len)
        .await
        .map(|buf_size| (buf_size, info_bits))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_size_len() {
        assert_eq!(buffer_size_len(0x00), 0); // 0
        assert_eq!(buffer_size_len(0x01), 1); // 1
        assert_eq!(buffer_size_len(0x02), 1); // 2
        assert_eq!(buffer_size_len(0xff), 1); // 255
        assert_eq!(buffer_size_len(0x100), 2); // 256
        assert_eq!(buffer_size_len(0xffff), 2); // 65535
        assert_eq!(buffer_size_len(0x010000), 3); // 65536
    }
}

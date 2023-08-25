use std::io::{Read, Write};

use byteorder::{ByteOrder, ReadBytesExt};

/// Get the size, in bytes, of the minimum number of bytes necessary to represent the specified
/// buffer size.
pub(crate) fn buffer_size_len(buf_size: u64) -> u8 {
    ((buf_size + 255) / 256) as u8
}

/// Write a buffer size to the specified `Write`.
///
/// The buffer size is written using the lowest possibly number of bytes, using the
/// least-significant bits to encode the buffer size length.
///
/// The `info_bits` is a 4 bits-max value that can be used to convey additional information into
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
    let mut size_buf = [0; 8];
    byteorder::NetworkEndian::write_u64(&mut size_buf, buf_size);

    let size_len = buffer_size_len(buf_size);

    // The size should never take more than 8 bytes, ensuring that the buffer size never exceeds 64
    // bits.
    if size_len > 8 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("size_len is too large ({size_len} > 8)"),
        ));
    }

    let idx = size_buf.len() - size_len as usize;

    assert!(info_bits <= 16, "info_bits should be stricly less than 16");

    let size_len = size_len | info_bits << 4;

    w.write_all(&[size_len])?;
    w.write_all(&size_buf[idx..])?;

    Ok(size_len as usize + 1)
}

/// Read a buffer size from the specified `Read`.
///
/// Returns `None` if the buffer size is not present.
///
/// The second element of the returned tuple are the info bits.
pub(crate) fn read_buffer_size(mut r: impl Read) -> std::io::Result<(Option<u64>, u8)> {
    let size_len = r.read_u8()?;

    let (info_bits, size_len) = (size_len >> 4, size_len & 0x0f);

    if size_len == 0 {
        return Ok((None, info_bits));
    }

    let mut size_buf = [0; 8];

    if size_len as usize > size_buf.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid size length",
        ));
    }

    r.read_exact(&mut size_buf[8 - size_len as usize..])?;
    let size = byteorder::NetworkEndian::read_u64(&size_buf);

    Ok((Some(size), info_bits))
}

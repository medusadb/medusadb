use std::io::{Read, Write};

use byteorder::{ReadBytesExt, WriteBytesExt};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{buf_utils, Cairn};

/// A `Ledger` is the definition for a blob of data, formed by other blobs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ledger {
    /// A linear aggregate of multiple blobs.
    LinearAggregate {
        /// The cairns for each blob of data that makes up this `Ledger`, in order.
        ///
        cairns: Vec<Cairn>,
    },
}

impl Ledger {
    const LINEAR_AGGREGATE: u8 = 0x00;

    /// Get the size of the resulting buffer when calling `write_to`.
    pub fn buf_len(&self) -> usize {
        match self {
            Self::LinearAggregate { cairns } => {
                1 + cairns.iter().map(|cairn| cairn.buf_len()).sum::<usize>()
            }
        }
    }

    /// Read a `Ledger` from the specified reader.
    pub fn read_from(mut r: impl Read) -> std::io::Result<Self> {
        match r.read_u8()? {
            Self::LINEAR_AGGREGATE => {
                let (cairns_count, info_bits) = buf_utils::read_buffer_size(&mut r)?;

                if info_bits != 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unexpected info bits of `{info_bits:02x}` when reading linear aggregate header"))
                    );
                }

                let mut cairns = Vec::with_capacity(
                    cairns_count
                        .try_into()
                        .expect("failed to convert u64 to usize"),
                );

                for _ in 0..cairns_count {
                    let cairn = Cairn::read_from(&mut r)?;
                    cairns.push(cairn);
                }

                Ok(Self::LinearAggregate { cairns })
            }
            x => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unknown ledger type `{x:02x}`"),
            )),
        }
    }

    /// Read a `Ledger` from the specified reader.
    pub async fn async_read_from(mut r: impl AsyncRead + Unpin + Send) -> std::io::Result<Self> {
        let mut buf = vec![0x00; 1];
        r.read_exact(&mut buf).await?;

        match buf[0] {
            Self::LINEAR_AGGREGATE => {
                let (cairns_count, info_bits) = buf_utils::async_read_buffer_size(&mut r).await?;

                if info_bits != 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unexpected info bits of `{info_bits:02x}` when reading linear aggregate header"))
                    );
                }

                let mut cairns = Vec::with_capacity(
                    cairns_count
                        .try_into()
                        .expect("failed to convert u64 to usize"),
                );

                for _ in 0..cairns_count {
                    let cairn = Cairn::async_read_from(&mut r).await?;
                    cairns.push(cairn);
                }

                Ok(Self::LinearAggregate { cairns })
            }
            x => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unknown ledger type `{x:02x}`"),
            )),
        }
    }

    /// Write the ledger to the specified writer.
    pub fn write_to(&self, mut w: impl Write) -> std::io::Result<usize> {
        match self {
            Self::LinearAggregate { cairns } => {
                let mut written = 1;

                w.write_u8(Self::LINEAR_AGGREGATE)?;

                written += buf_utils::write_buffer_size(
                    &mut w,
                    cairns
                        .len()
                        .try_into()
                        .expect("failed to convert usize to u64"),
                    0x00,
                )?;

                for cairn in cairns {
                    written += cairn.write_to(&mut w)?;
                }

                Ok(written)
            }
        }
    }

    /// Write the ledger to the specified writer.
    pub async fn async_write_to(&self, mut w: impl AsyncWrite + Unpin) -> std::io::Result<usize> {
        match self {
            Self::LinearAggregate { cairns } => {
                let mut written = 1;

                w.write_all(&[Self::LINEAR_AGGREGATE]).await?;

                written += buf_utils::async_write_buffer_size(
                    &mut w,
                    cairns
                        .len()
                        .try_into()
                        .expect("failed to convert usize to u64"),
                    0x00,
                )
                .await?;

                for cairn in cairns {
                    written += cairn.async_write_to(&mut w).await?;
                }

                Ok(written)
            }
        }
    }

    /// Serialize the ledger as a vector of bytes.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.buf_len());

        self.write_to(&mut buf)
            .expect("writing to a memory buffer should never fail");

        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ledger() {
        let expected = vec![0, 1, 2, 2, 1, 2, 2, 3, 4];
        //                  ^  ^     ^        ^
        //                  |  |     |        \- Second cairn.
        //                  |  |     |
        //                  |  |     \- First cairn.
        //                  |  |
        //                  |  \- Number of cairns in the ledger, with prefix.
        //                  |
        //                  \- Type of ledger (linear aggregated).

        let ledger = Ledger::LinearAggregate {
            cairns: vec![
                Cairn::self_contained(vec![0x01, 0x02]).unwrap(),
                Cairn::self_contained(vec![0x03, 0x04]).unwrap(),
            ],
        };

        {
            assert_eq!(ledger.to_vec(), expected);
            let other = Ledger::read_from(std::io::Cursor::new(&expected)).unwrap();

            assert_eq!(other, ledger);
        }

        {
            let mut buf = Vec::with_capacity(ledger.buf_len());
            ledger.async_write_to(&mut buf).await.unwrap();

            assert_eq!(ledger.to_vec(), expected);
            let other = Ledger::read_from(std::io::Cursor::new(&expected)).unwrap();

            assert_eq!(other, ledger);
        }
    }
}

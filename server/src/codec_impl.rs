use anyhow::{Result, bail};

pub fn decode_b64(value: &str) -> Result<usize> {
    let mut total = 0usize;
    for (power, byte) in value.bytes().rev().enumerate() {
        if byte < 64 {
            bail!("invalid B64 byte {byte}");
        }
        total += ((byte - 64) as usize) * 64usize.pow(power as u32);
    }
    Ok(total)
}

pub fn encode_vl64(mut value: i32) -> String {
    let negative_mask = if value >= 0 { 0 } else { 4 };
    value = value.abs();

    let mut out = [0u8; 6];
    let mut pos = 0usize;
    out[pos] = 64 + (value as u8 & 3);
    pos += 1;
    value >>= 2;

    let mut bytes = 1u8;
    while value != 0 {
        out[pos] = 64 + (value as u8 & 0x3f);
        pos += 1;
        value >>= 6;
        bytes += 1;
    }

    out[0] |= (bytes << 3) | negative_mask;
    String::from_utf8_lossy(&out[..pos]).to_string()
}

pub fn decode_vl64(data: &str) -> Result<(i32, usize)> {
    let raw = data.as_bytes();
    if raw.is_empty() {
        bail!("empty VL64 input");
    }

    let first = raw[0];
    let negative = (first & 4) == 4;
    let total_bytes = ((first >> 3) & 7) as usize;
    if total_bytes == 0 || raw.len() < total_bytes {
        bail!("invalid VL64 byte count");
    }

    let mut value = (first & 3) as i32;
    for (index, byte) in raw.iter().copied().enumerate().take(total_bytes).skip(1) {
        value |= ((byte & 0x3f) as i32) << (2 + 6 * (index - 1));
    }

    if negative {
        value *= -1;
    }

    Ok((value, total_bytes))
}

pub fn legacy_frame(payload: &str) -> Vec<u8> {
    let mut bytes = payload.as_bytes().to_vec();
    bytes.push(0x01);
    bytes
}

#[derive(Default)]
pub struct PacketBuffer {
    inner: String,
}

impl PacketBuffer {
    pub fn push(&mut self, bytes: &[u8]) {
        self.inner.push_str(&String::from_utf8_lossy(bytes));
    }

    pub fn next_packets(&mut self) -> Result<Vec<String>> {
        let mut packets = Vec::new();

        loop {
            if self.inner.len() < 3 {
                break;
            }

            let length = decode_b64(&self.inner[1..3])?;
            let frame_len = length + 3;
            if self.inner.len() < frame_len {
                break;
            }

            let payload = self.inner[3..frame_len].to_string();
            self.inner.drain(..frame_len);
            packets.push(payload);
        }

        Ok(packets)
    }
}

#[cfg(test)]
mod tests {
    use super::{PacketBuffer, decode_b64, decode_vl64, encode_vl64};

    #[test]
    fn decodes_b64_lengths() {
        assert_eq!(decode_b64("@A").unwrap(), 1);
        assert_eq!(decode_b64("@B").unwrap(), 2);
    }

    #[test]
    fn encodes_vl64() {
        assert_eq!(encode_vl64(1), "I");
        assert_eq!(decode_vl64("I").unwrap(), (1, 1));
    }

    #[test]
    fn buffers_packets() {
        let mut buffer = PacketBuffer::default();
        buffer.push(b"A@BCD");
        let packets = buffer.next_packets().unwrap();
        assert_eq!(packets, vec!["CD".to_string()]);
    }

    #[test]
    fn buffers_multiple_packets() {
        let mut buffer = PacketBuffer::default();
        buffer.push(b"A@BCDA@BCN");
        let packets = buffer.next_packets().unwrap();
        assert_eq!(packets, vec!["CD".to_string(), "CN".to_string()]);
    }
}

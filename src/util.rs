pub trait ValidGraphType:
    Copy + std::str::FromStr + std::cmp::PartialOrd + std::ops::Add<Output = Self> + TryInto<usize> + std::fmt::Display
{
    fn zero() -> Self;
    fn one() -> Self;
    fn serialize(&self) -> Vec<u8>;
    fn count(&self) -> usize;
    fn from_bytes(bytes: &[u8]) -> Self;
}

impl ValidGraphType for u64 {
    fn zero() -> Self {
        0
    }
    fn one() -> Self {
        1
    }
    fn serialize(&self) -> Vec<u8> {
        Vec::from(self.to_ne_bytes())
    }
    fn count(&self) -> usize {
        *self as usize
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        let mut arr = [0u8; std::mem::size_of::<Self>()];
        for i in 0..std::mem::size_of::<Self>() {
            arr[i] = bytes[i];
        }

        u64::from_ne_bytes(arr)
    }
}

impl ValidGraphType for u32 {
    fn zero() -> Self {
        0
    }
    fn one() -> Self {
        1
    }
    fn serialize(&self) -> Vec<u8> {
        Vec::from(self.to_ne_bytes())
    }
    fn count(&self) -> usize {
        *self as usize
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        let mut arr = [0u8; std::mem::size_of::<Self>()];
        for i in 0..std::mem::size_of::<Self>() {
            arr[i] = bytes[i];
        }

        u32::from_ne_bytes(arr)
    }
}

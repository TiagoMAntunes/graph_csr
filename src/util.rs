pub trait ValidGraphType: Copy + std::str::FromStr + std::cmp::PartialOrd + std::ops::Add<Output = Self> {
    fn zero() -> Self;
    fn one() -> Self;
    fn serialize(&self) -> Vec<u8>;
    fn count(&self) -> usize;
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
}

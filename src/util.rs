/// This trait is used for convenience in implementing the types accepted by the graph.
/// The compiler is still rather limited in some aspects of writing generic code in binary format, so this works as a temporary workaround.
/// It is not expected that anyone will use this trait directly, as u64 will cover most use cases for large graphs.
pub trait ValidGraphType:
    Copy
    + std::str::FromStr
    + std::fmt::Display
    + num_traits::Num
    + num_traits::AsPrimitive<usize>
    + std::cmp::PartialOrd
{
    fn serialize(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Self;
}

impl ValidGraphType for u64 {
    fn serialize(&self) -> Vec<u8> {
        Vec::from(self.to_ne_bytes())
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
    fn serialize(&self) -> Vec<u8> {
        Vec::from(self.to_ne_bytes())
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        let mut arr = [0u8; std::mem::size_of::<Self>()];
        for i in 0..std::mem::size_of::<Self>() {
            arr[i] = bytes[i];
        }

        u32::from_ne_bytes(arr)
    }
}

/// The data present in each vertex
pub trait GraphData: Copy + Default + PartialEq + PartialOrd + Send + Sync {}

impl GraphData for u32 {}
impl GraphData for u64 {}
impl GraphData for f32 {}
impl GraphData for f64 {}

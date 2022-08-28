use std::{
    fs,
    io::{BufReader, BufWriter, Read, Result, Write},
    marker::PhantomData,
};

use super::util;

const VERTEX_NAME: &'static str = "vertex.csr";
const EDGE_NAME: &'static str = "edge.csr";

/// A graph's metadata
pub struct GraphFiles(pub fs::File, pub fs::File, pub usize, pub usize);

/// Convenience function to create a new vertex file in the `folder_name` directory.
pub fn get_vertex_file(folder_name: &str) -> Result<fs::File> {
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(format!("{}/{}", folder_name, VERTEX_NAME))
}

/// Convenience function to create a new edge file in the `folder_name` directory.
pub fn get_edge_file(folder_name: &str) -> Result<fs::File> {
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(format!("{}/{}", folder_name, EDGE_NAME))
}

/// General function that describes the behaviour of the graph.
/// Must receive an iterator that yields `std::io::Result<(N,N)>`.
pub fn from_adjacency_list<N, T>(
    stream: T,
    destination_folder_name: &str,
) -> std::io::Result<GraphFiles>
where
    T: Iterator<Item = std::io::Result<(N, N)>> + Sized,
    N: util::ValidGraphType,
{
    // Create directory if does not exist
    fs::create_dir(destination_folder_name)?;

    // Create the files and buffers to write the data to
    let nodes_file = get_vertex_file(destination_folder_name)?;
    let edges_file = get_edge_file(destination_folder_name)?;
    let mut nodes_writer = BufWriter::new(&nodes_file);
    let mut edges_writer = BufWriter::new(&edges_file);

    let mut previous_node = N::zero();
    let mut edges_count = N::zero();
    let mut max = N::zero();

    nodes_writer
        .write(&N::zero().serialize())
        .expect("Failed to write first node");

    for e in stream {
        let (src, dst) = match e {
            Ok((_src, _dst)) => (_src, _dst),
            Err(e) => Err(e)?,
        };

        if max < dst {
            max = dst;
        }

        // Check if sorted by source
        if src < previous_node {
            Err(std::io::ErrorKind::InvalidData)?;
        }

        // Write edge to edge list
        edges_writer.write(&dst.serialize())?;

        // Write missing nodes
        while previous_node < src {
            previous_node = previous_node + N::one();
            nodes_writer.write(&edges_count.serialize())?;
        }

        edges_count = edges_count + N::one();
        previous_node = src;
    }

    let max = max + N::one();

    // Add nodes until we reach the max node
    while previous_node < max {
        previous_node = previous_node + N::one();
        nodes_writer.write(&edges_count.serialize())?;
    }

    edges_writer.flush()?;
    nodes_writer.flush()?;

    drop(edges_writer);
    drop(nodes_writer);

    Ok(GraphFiles(
        nodes_file,
        edges_file,
        max.count() + 1,
        edges_count.count(),
    ))
}

/// This struct can be used to parse a binary reader into pairs of (T, T).
pub struct ReaderIterator<T, K>
where
    T: util::ValidGraphType,
    K: Read,
{
    reader: BufReader<K>,
    buffer: Vec<u8>,
    _phantom: PhantomData<T>,
}

/// Creates a new ReaderIterator from `reader` that yields pairs (T,T). K must be a type that implements the `Read` trait.
pub fn reader_to_iter<T, K>(reader: K) -> ReaderIterator<T, impl Read>
where
    T: Sized + util::ValidGraphType,
    K: Read,
{
    ReaderIterator {
        reader: BufReader::new(reader),
        _phantom: PhantomData,
        buffer: vec![0u8; std::mem::size_of::<T>()],
    }
}

impl<T, K> Iterator for ReaderIterator<T, K>
where
    T: Sized + util::ValidGraphType,
    K: Read,
{
    type Item = (T, T);

    fn next(&mut self) -> Option<Self::Item> {
        let v1 = match self.reader.read_exact(&mut self.buffer) {
            Ok(_) => Some(T::from_bytes(&self.buffer)),
            Err(_) => None,
        };

        let v2 = match self.reader.read_exact(&mut self.buffer) {
            Ok(_) => Some(T::from_bytes(&self.buffer)),
            Err(_) => None,
        };

        match (v1, v2) {
            (Some(v1), Some(v2)) => Some((v1, v2)),
            _ => None,
        }
    }
}

use std::{
    fs,
    io::{BufWriter, Result, Write},
};

use super::util;

const VERTEX_NAME: &'static str = "vertex.csr";
const EDGE_NAME: &'static str = "edge.csr";

pub struct GraphFiles(pub fs::File, pub fs::File, pub usize, pub usize);

fn get_vertex_file(folder_name: &str) -> Result<fs::File> {
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(format!("{}/{}", folder_name, VERTEX_NAME))
}

fn get_edge_name(folder_name: &str) -> Result<fs::File> {
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(format!("{}/{}", folder_name, EDGE_NAME))
}

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
    let edges_file = get_edge_name(destination_folder_name)?;
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
            Err(e) => Err(e)?
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

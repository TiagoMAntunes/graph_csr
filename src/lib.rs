use std::{
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    str::FromStr,
};

use easy_mmap::{self, EasyMmap, EasyMmapBuilder};

/// This structure holds a graph in the Compressed Sparse Row format for compression of data size.
/// This graph is represented via Memory Mapping, allowing the graph to be loaded into memory as required.
/// This makes it possible to load any-size graphs, even those that *do not* fit into memory!
pub struct Graph<'a, N> {
    nodes: EasyMmap<'a, N>,
    edges: EasyMmap<'a, N>,
}

pub trait GraphType: Copy + FromStr + std::cmp::PartialOrd + std::ops::Add<Output = Self> {
    fn zero() -> Self;
    fn one() -> Self;
    fn serialize(&self) -> Vec<u8>;
    fn count(&self) -> usize;
}

impl GraphType for u64 {
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

impl GraphType for u32 {
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

/// The errors that can occur from using this library
#[derive(Debug)]
pub enum GraphError {
    /// The specified file has not been able to be opened
    ErrOpeningFile,
    /// Errors when parsing the source file
    ParseError,
    /// Errors when loading the underlying data file
    LoadError,
    /// General error when communicating with the filesystem, such as failed to create a directory
    FsError,
}

impl<'a, N> Graph<'a, N>
where
    N: GraphType + std::fmt::Display,
    N: 'a,
{
    fn get_file(folder_name: &str, file_name: &str) -> Result<File, GraphError> {
        match fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(format!("{}/{}", folder_name, file_name))
        {
            Ok(file) => Ok(file),
            Err(_) => Err(GraphError::ErrOpeningFile),
        }
    }

    fn get_node_file(folder_name: &str) -> Result<File, GraphError> {
        // Self::create_writer(folder_name, "vertex.csr")
        Self::get_file(folder_name, "vertex.csr")
    }

    fn get_edge_file(folder_name: &str) -> Result<File, GraphError> {
        Self::get_file(folder_name, "edge.csr")
    }

    /// Given a SORTED (by source) adjancency list file `source_file_name`, transforms this file
    /// into the underlying binary representation in CSR and returns a version of the Graph in this format
    pub fn from_txt_adjancency(
        source_file_name: &str,
        destination_folder_name: &str,
    ) -> Result<Graph<'a, N>, GraphError> {
        let source_file = match fs::OpenOptions::new().read(true).open(source_file_name) {
            Ok(f) => Ok(f),
            Err(_) => Err(GraphError::ErrOpeningFile),
        }?;

        let reader = BufReader::new(source_file);

        // Create directory if does not exist
        match fs::create_dir(destination_folder_name) {
            Ok(_) => Ok(()),
            Err(_) => Err(GraphError::FsError),
        }?;

        // Create the files and buffers to write the data to
        let nodes_file = Self::get_node_file(destination_folder_name)?;
        let edges_file = Self::get_edge_file(destination_folder_name)?;
        let mut nodes_writer = BufWriter::new(&nodes_file);
        let mut edges_writer = BufWriter::new(&edges_file);

        let mut previous_node = N::zero();
        let mut edges_count = N::zero();
        let mut max = N::zero();

        nodes_writer.write(&N::zero().serialize()).expect("Failed to write first node");

        for line in reader.lines() {
            let line = match line {
                Ok(line) => Ok(line),
                Err(_) => Err(GraphError::ParseError),
            }?;

            // Parse the two values into the correct format
            let (src, dst): (N, N) = match line.split_whitespace().collect::<Vec<_>>().as_slice() {
                [src, dst] => {
                    let src = match src.parse::<N>() {
                        Ok(src) => Ok(src),
                        Err(_) => Err(GraphError::ParseError),
                    }?;
                    let dst = match dst.parse::<N>() {
                        Ok(dst) => Ok(dst),
                        Err(_) => Err(GraphError::ParseError),
                    }?;
                    (src, dst)
                }
                _ => {
                    return Err(GraphError::ParseError);
                }
            };

            if max < dst {
                max = dst;
            }

            // Check if sorted by source
            if src < previous_node {
                return Err(GraphError::ParseError);
            }

            // Write edge to edge list
            match edges_writer.write(&dst.serialize()) {
                Ok(_) => Ok(()),
                Err(_) => Err(GraphError::LoadError),
            }?;

            while previous_node < src {
                previous_node = previous_node + N::one();
                // Write node size to nodes
                println!("{}", edges_count);
                match nodes_writer.write(&edges_count.serialize()) {
                    Ok(_) => Ok(()),
                    Err(_) => Err(GraphError::LoadError),
                }?;
            }

            edges_count = edges_count + N::one();
            previous_node = src;
        }

        let max = max + N::one();

        // Add nodes until we reach the max node
        while previous_node < max {
            previous_node = previous_node + N::one();
            // Write node size to nodes
            println!("{}", edges_count);
            match nodes_writer.write(&edges_count.serialize()) {
                Ok(_) => Ok(()),
                Err(_) => Err(GraphError::LoadError),
            }?;
        }

        // Write the last node
        println!("{}", edges_count);

        match edges_writer.flush() {
            Ok(_) => Ok(()),
            Err(_) => Err(GraphError::LoadError),
        }?;
        match nodes_writer.flush() {
            Ok(_) => Ok(()),
            Err(_) => Err(GraphError::LoadError),
        }?;
        drop(edges_writer);
        drop(nodes_writer);

        // Create final graph
        Ok(Graph {
            nodes: EasyMmapBuilder::new()
                .readable()
                .capacity(max.count() + 1usize)
                .file(nodes_file)
                .build(),
            edges: EasyMmapBuilder::new()
                .readable()
                .capacity(edges_count.count())
                .file(edges_file)
                .build(),
        })
    }

    /// Same as `from_txt_adjacency`, except this time it assumes the edge list to be in binary representation
    pub fn from_binary_adjancency(
        source_file_name: &str,
        destination_folder_name: &str,
    ) -> Result<Graph<'a, N>, GraphError> {
        todo!()
    }

    pub fn load_graph(graph_folder: &str) -> Result<Graph<'a, N>, GraphError> {
        todo!()
    }

    pub fn iterate_nodes(&self) -> std::slice::Iter<'_, N> {
        self.nodes.iter()
    }

    pub fn iterate_edges(&self) -> std::slice::Iter<'_, N> {
        self.edges.iter()
    }
}

#[cfg(test)]
mod tests {
    use core::panic;

    use super::*;

    #[test]
    fn test_basic_parse() {
        let edges = vec![(0u32, 1u32), (0, 2), (1, 5), (1, 2), (4, 7)];

        let expected_nodes = vec![0u32, 2, 4, 4, 4, 5, 5, 5, 5];
        let expected_edges = vec![1u32, 2, 5, 2, 7];

        let source_file_name = format!("/tmp/tmp_src_{}", rand::random::<u32>());
        let destination_folder_name = format!("/tmp/tmp_dst_{}", rand::random::<u32>());

        let file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&source_file_name)
            .unwrap();

        // Write edges to file
        let mut writer = BufWriter::new(&file);
        for edge in edges {
            let line = format!("{} {}\n", edge.0, edge.1);
            writer.write(line.as_bytes()).unwrap();
        }

        drop(writer);

        println!(
            "Filenames: {} {}",
            source_file_name, destination_folder_name
        );

        let graph =
            match Graph::<u32>::from_txt_adjancency(&source_file_name, &destination_folder_name) {
                Ok(graph) => graph,
                Err(e) => panic!("{:?}", e),
            };

        // Check correctness
        assert_eq!(
            graph
                .iterate_nodes()
                .map(|x| x.clone())
                .collect::<Vec<u32>>(),
            expected_nodes
        );
        assert_eq!(
            graph
                .iterate_edges()
                .map(|x| x.clone())
                .collect::<Vec<u32>>(),
            expected_edges
        );
    }
}

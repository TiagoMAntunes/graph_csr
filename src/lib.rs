use std::io::{BufRead, BufReader, Read};

use easy_mmap::{self, EasyMmap, EasyMmapBuilder};

mod reading;
mod util;

/// This structure holds a graph in the Compressed Sparse Row format for compression of data size.
/// This graph is represented via Memory Mapping, allowing the graph to be loaded into memory as required.
/// This makes it possible to load any-size graphs, even those that *do not* fit into memory!
pub struct Graph<'a, N> {
    nodes: EasyMmap<'a, N>,
    edges: EasyMmap<'a, N>,
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
    N: util::ValidGraphType + std::fmt::Display,
    N: 'a,
{
    /// Convenience method for reading an input stream in text format.
    /// Each line should contain two numbers, separated by a space.
    pub fn from_txt_adjacency_list<T>(
        stream: T,
        folder_name: &str,
    ) -> Result<Graph<'a, N>, GraphError>
    where
        T: Read + Sized,
    {
        let reader = BufReader::new(stream);
        let stream = reader.lines().map(|line| {
            let line = line?;
            let mut parts = line.split_whitespace();

            let src = parts
                .next()
                .ok_or(std::io::ErrorKind::InvalidData)?
                .parse::<N>()
                .or(Err(std::io::ErrorKind::InvalidData))?;

            let dst = parts
                .next()
                .ok_or(std::io::ErrorKind::InvalidData)?
                .parse::<N>()
                .or(Err(std::io::ErrorKind::InvalidData))?;

            std::io::Result::Ok((src, dst))
        });
        Graph::from_adjacency_list(stream, folder_name)
    }

    /// Given a SORTED (by source) adjancency list file `source_file_name`, transforms this file
    /// into the underlying binary representation in CSR and returns a version of the Graph in this format
    pub fn from_adjacency_list<T>(stream: T, folder_name: &str) -> Result<Graph<'a, N>, GraphError>
    where
        T: Iterator<Item = std::io::Result<(N, N)>> + Sized,
    {
        let reading::GraphFiles(vertex_file, edge_file, n_vertex, n_edges) =
            match reading::from_adjacency_list::<N, T>(stream, folder_name) {
                Ok(g) => Ok(g),
                Err(e) => match e.kind() {
                    std::io::ErrorKind::InvalidData => Err(GraphError::ParseError),
                    _ => Err(GraphError::LoadError),
                },
            }?;

        let nodes = EasyMmapBuilder::<N>::new()
            .file(vertex_file)
            .readable()
            .capacity(n_vertex)
            .build();
        let edges = EasyMmapBuilder::<N>::new()
            .file(edge_file)
            .readable()
            .capacity(n_edges)
            .build();

        Ok(Graph { nodes, edges })
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
    use std::io::{BufWriter, Write};
    use std::fs;

    use super::*;

    #[test]
    fn parse_from_file() {
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

        let file = fs::OpenOptions::new()
            .read(true)
            .open(&source_file_name)
            .unwrap();

        let graph = match Graph::<u32>::from_txt_adjacency_list(file, &destination_folder_name) {
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

    #[test]
    fn parse_from_general_stream() {
        let edges = vec![(0u32, 1u32), (0, 2), (1, 5), (1, 2), (4, 7)];

        let expected_nodes = vec![0u32, 2, 4, 4, 4, 5, 5, 5, 5];
        let expected_edges = vec![1u32, 2, 5, 2, 7];

        let destination_folder_name = format!("/tmp/tmp_dst_{}", rand::random::<u32>());

        // Read from string bytes stream
        let graph = match Graph::<u32>::from_adjacency_list(
            edges.iter().map(|x| Ok(x.clone())),
            &destination_folder_name,
        ) {
            Ok(graph) => graph,
            Err(e) => panic!("{:?}", e),
        };

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

    #[test]
    fn load_u64_graph() {
        let edges = vec![(0u64, 1u64), (0, 2), (1, 5), (1, 2), (4, 7)];

        let expected_nodes = vec![0u64, 2, 4, 4, 4, 5, 5, 5, 5];
        let expected_edges = vec![1u64, 2, 5, 2, 7];
        
        let destination_folder_name = format!("/tmp/tmp_dst_{}", rand::random::<u32>());

        let graph = match Graph::<u64>::from_adjacency_list(
            edges.iter().map(|x| Ok(x.clone())),
            &destination_folder_name,
        ) {
            Ok(graph) => graph,
            Err(e) => panic!("{:?}", e),
        };

        assert_eq!(
            graph
                .iterate_nodes()
                .map(|x| x.clone())
                .collect::<Vec<u64>>(),
            expected_nodes
        );

        assert_eq!(
            graph
                .iterate_edges()
                .map(|x| x.clone())
                .collect::<Vec<u64>>(),
            expected_edges
        );
    }
}

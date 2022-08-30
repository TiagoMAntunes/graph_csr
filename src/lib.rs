use std::io::{BufRead, BufReader, Read};

use easy_mmap::{self, EasyMmap, EasyMmapBuilder};
use reading::reader_to_iter;
use util::ValidGraphType;

mod reading;

/// The generalized computational scheme for running algorithms
pub mod compute;

/// A collection of convenient functions and traits to be used across the crate.
pub mod util;

/// This structure holds a graph in the Compressed Sparse Row format for compression of data size.
/// This graph is represented via Memory Mapping, allowing the graph to be loaded into memory as required.
/// This makes it possible to load any-size graphs, even those that *do not* fit into memory!
pub struct Graph<'a, N> {
    nodes: EasyMmap<'a, usize>,
    edges: EasyMmap<'a, N>,
}

impl<'a, N> Graph<'a, N>
where
    N: util::ValidGraphType,
    N: 'a,
{
    /// Convenience method for reading an input stream in text format.
    /// Each line should contain two numbers, separated by a space.
    /// The graph will be converted to the underlying CSR representation, and stored in `folder_name`.
    pub fn from_txt_adjacency_list<T>(
        stream: T,
        folder_name: &str,
    ) -> Result<Graph<'a, N>, std::io::Error>
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

    /// Same as [from_txt_adjacency](Self::from_txt_adjacency_list), except this time it assumes the edge list to be in binary representation.
    pub fn from_binary_adjancency<T>(
        stream: T,
        destination_folder_name: &str,
    ) -> Result<Graph<'a, N>, std::io::Error>
    where
        T: Read + Sized,
    {
        Graph::from_adjacency_list(
            reader_to_iter::<N, T>(stream).map(|x| std::io::Result::Ok(x)),
            destination_folder_name,
        )
    }

    /// Given a SORTED (by source) adjancency list file `source_file_name`, transforms this file
    /// into the underlying binary representation in CSR and returns a version of the Graph in this format.
    /// The graph will be stored in `folder_name`.
    pub fn from_adjacency_list<T>(
        stream: T,
        folder_name: &str,
    ) -> Result<Graph<'a, N>, std::io::Error>
    where
        T: Iterator<Item = std::io::Result<(N, N)>> + Sized,
    {
        reading::from_adjacency_list::<N, T>(stream, folder_name)?;

        Self::load_graph(folder_name)
    }

    /// Loads a graph from the underlying representation and returns it as a `Graph` struct.
    pub fn load_graph(graph_folder: &str) -> Result<Graph<'a, N>, std::io::Error> {
        let nodes_file = reading::get_vertex_file(graph_folder)?;
        let edges_file = reading::get_edge_file(graph_folder)?;

        let nodes = EasyMmapBuilder::<usize>::new()
            .capacity(
                nodes_file
                    .metadata()
                    .expect("Failed to read metadata of vertex file")
                    .len() as usize
                    / std::mem::size_of::<usize>(),
            )
            .file(nodes_file)
            .readable()
            .build();

        let edges = EasyMmapBuilder::<N>::new()
            .capacity(
                edges_file
                    .metadata()
                    .expect("Failed to read metadata of edge file")
                    .len() as usize
                    / std::mem::size_of::<N>(),
            )
            .file(edges_file)
            .readable()
            .build();

        Ok(Graph { nodes, edges })
    }

    /// Returns an iterator over the edge list of each node.
    pub fn iter(&'a self) -> GraphIterator<'a, N> {
        GraphIterator {
            nodes: self.nodes.get_data_as_slice(),
            edges: self.edges.get_data_as_slice(),
            current_node: 0,
        }
    }

    #[inline]
    #[allow(dead_code)]
    fn iterate_nodes(&'a self) -> impl Iterator<Item = usize> + 'a {
        self.nodes.iter().map(|x| *x)
    }

    #[inline]
    #[allow(dead_code)]
    fn iterate_edges(&'a self) -> impl Iterator<Item = N> + 'a {
        self.edges.iter().map(|x| *x)
    }

    /// Returns the number of nodes existing in the graph
    pub fn n_nodes(&self) -> usize {
        self.nodes.len() - 1
    }

    /// Returns the number of edges existing in the graph
    pub fn n_edges(&self) -> usize {
        self.edges.len()
    }
}

/// Iterates over a [Graph] struct and yields the outgoing edge lists of type `&[N]` for each node.
pub struct GraphIterator<'a, N> {
    nodes: &'a [usize],
    edges: &'a [N],
    current_node: usize,
}

impl<'a, N> Iterator for GraphIterator<'a, N>
where
    N: ValidGraphType,
{
    type Item = &'a [N];

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_node >= self.nodes.len() - 1 {
            return None;
        };

        let start = self.nodes[self.current_node];
        let end = self.nodes[self.current_node + 1];

        self.current_node += 1;

        Some(&self.edges[start..end])
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{BufWriter, Write};

    use super::*;

    #[test]
    fn parse_from_file() {
        let edges = vec![(0u32, 1u32), (0, 2), (1, 5), (1, 2), (4, 7)];

        let expected_nodes = vec![0usize, 2, 4, 4, 4, 5, 5, 5, 5];
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
                .collect::<Vec<usize>>(),
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
    fn parse_from_binary() {
        let edges = vec![(0u32, 1u32), (0, 2), (1, 5), (1, 2), (4, 7)];
        let expected_nodes = vec![0usize, 2, 4, 4, 4, 5, 5, 5, 5];
        let expected_edges = vec![1u32, 2, 5, 2, 7];

        let destionation_folder_name = format!("/tmp/tmp_dst_{}", rand::random::<u32>());

        let edges_flatten = edges
            .iter()
            .flat_map(|x| vec![x.0, x.1])
            .collect::<Vec<u32>>();

        // Transform into slice of u8 for Read
        let edges_flatten = unsafe {
            std::slice::from_raw_parts(
                edges_flatten.as_ptr() as *const u8,
                edges_flatten.len() * std::mem::size_of::<u32>(),
            )
        };

        let graph =
            match Graph::<u32>::from_binary_adjancency(edges_flatten, &destionation_folder_name) {
                Ok(graph) => graph,
                Err(e) => panic!("{:?}", e),
            };

        // Check correctness
        assert_eq!(
            graph
                .iterate_nodes()
                .map(|x| x.clone())
                .collect::<Vec<usize>>(),
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

        let expected_nodes = vec![0usize, 2, 4, 4, 4, 5, 5, 5, 5];
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

        println!("Destionation folder: {}", destination_folder_name);

        assert_eq!(
            graph
                .iterate_nodes()
                .map(|x| x.clone())
                .collect::<Vec<usize>>(),
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

        let expected_nodes = vec![0usize, 2, 4, 4, 4, 5, 5, 5, 5];
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
                .collect::<Vec<usize>>(),
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

    #[test]
    fn test_graph_load() {
        let edges = vec![(0u32, 1u32), (0, 2), (1, 5), (1, 2), (4, 7)];
        let expected_nodes = vec![0usize, 2, 4, 4, 4, 5, 5, 5, 5];
        let expected_edges = vec![1u32, 2, 5, 2, 7];

        let destination_folder_name = format!("/tmp/tmp_dst_{}", rand::random::<u32>());

        match Graph::<u32>::from_adjacency_list(
            edges.iter().map(|x| Ok(x.clone())),
            &destination_folder_name,
        ) {
            Ok(_) => {}
            Err(e) => panic!("{:?}", e),
        };

        // Load graph from memory
        let graph = match Graph::<u32>::load_graph(&destination_folder_name) {
            Ok(graph) => graph,
            Err(e) => panic!("{:?}", e),
        };

        assert_eq!(
            graph
                .iterate_nodes()
                .map(|x| x.clone())
                .collect::<Vec<usize>>(),
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
    fn iterate_graph() {
        let edges = vec![(0u32, 1u32), (0, 2), (1, 5), (1, 2), (4, 7)];
        let expected_res = vec![
            (0usize, vec![1, 2]),
            (1, vec![5, 2]),
            (2, vec![]),
            (3, vec![]),
            (4, vec![7]),
            (5, vec![]),
            (6, vec![]),
            (7, vec![]),
        ];

        let destination_folder_name = format!("/tmp/tmp_dst_{}", rand::random::<u32>());

        let graph = match Graph::<u32>::from_adjacency_list(
            edges.iter().map(|x| Ok(x.clone())),
            &destination_folder_name,
        ) {
            Ok(g) => g,
            Err(e) => panic!("{:?}", e),
        };

        assert_eq!(
            graph
                .iter()
                .enumerate()
                .map(|(i, edges)| (i, edges.to_vec()))
                .collect::<Vec<(usize, Vec<u32>)>>(),
            expected_res
        );
    }

    #[test]
    fn invalid() {}
}

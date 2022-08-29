use std::ops::AddAssign;

use super::*;

pub trait Algorithm {}

pub trait GraphData: Copy + Default + Sync + AddAssign + PartialEq {}

impl GraphData for u32 {}
impl GraphData for u64 {}
impl GraphData for f32 {}
impl GraphData for f64 {}

pub struct ComputeGraph<'a, T, DataType> {
    graph: &'a Graph<'a, T>,
    old_active: Vec<bool>, // which nodes are active in the old
    new_active: Vec<bool>, // which nodes are active in the new iteration
    old_data: Vec<DataType>,  // the data associated with each node
    new_data: Vec<DataType>,  // the data associated with each node
}

impl<'a, T, DataType> ComputeGraph<'a, T, DataType>
where
    T: ValidGraphType,
    DataType: GraphData,
{
    /// Creates a new graph that can run algorithms and can keep track of node data as well as the active nodes.
    pub fn new(graph: &'a Graph<'a, T>) -> Self {
        let n_nodes = graph.n_nodes();
        Self {
            graph,
            old_active: vec![false; n_nodes],
            new_active: vec![false; n_nodes],
            old_data: vec![Default::default(); n_nodes],
            new_data: vec![Default::default(); n_nodes],
        }
    }

    /// Set a single node as active status.
    #[inline]
    pub fn set_active(&mut self, idx: usize, status: bool) {
        self.new_active[idx] = status;
    }

    /// Sets a single node's data.
    #[inline]
    pub fn set_data(&mut self, idx: usize, data: DataType) {
        self.new_data[idx] = data;
    }

    /// Performs a global iteration step, useful in many algorithms.
    /// The previous status of all nodes is now updated to the new status.
    /// The new status is reset to false.
    pub fn step(&mut self) {
        // Swap old and new
        std::mem::swap(&mut self.old_active, &mut self.new_active);
        std::mem::swap(&mut self.old_data, &mut self.new_data);

        // Set new all to false
        self.new_active.iter_mut().for_each(|x| *x = false);
        self.new_data
            .iter_mut()
            .zip(self.old_data.iter())
            .for_each(|(x, y)| *x = *y);
    }

    /// Returns how many nodes are active in the last iteration.
    /// This function calculates the value every time, so it is recommended to store its value.
    pub fn n_active(&self) -> usize {
        self.old_active.iter().filter(|x| **x).count()
    }

    /// This is the abstraction over the graph that follows the Think-Like-A-Vertex paradigm.
    /// `func` must be a function applied from the perspective of a single node.
    /// Each vertex knows its data and the data of its neighbour, and must return the new value following the algorithm.
    pub fn push<F>(&mut self, mut func: F)
    where
        F: FnMut(DataType, &mut DataType),
    {
        self.graph
            .iter()
            .zip(self.old_active.iter_mut())
            .zip(self.old_data.iter())
            .map(|((edges, active), data)| (edges, active, data))
            .filter(|(_, active, _)| **active)
            .map(|(edges, _, data)| (edges, data))
            .for_each(|(edges, local_data)| {
                for edge in edges {
                    // Update the data
                    let old = self.new_data[edge.as_()];
                    func(*local_data, &mut self.new_data[edge.as_()]);

                    // If it's different than before, then the node is now active
                    if self.new_data[edge.as_()] != old {
                        self.new_active[edge.as_()] = true;
                    }
                }
            })
    }

    pub fn get_data_as_slice(&self) -> &[DataType] {
        &self.old_data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_basic_graph<'a>() -> Graph<'a, u32> {
        // Default graph
        let edges = vec![(0u32, 1u32), (0, 2), (1, 5), (1, 2), (4, 7)];

        get_graph(edges)
    }

    fn get_graph<'a, T>(edge_list: Vec<(T, T)>) -> Graph<'a, T>
    where
        T: ValidGraphType,
    {
        // Generate random filename
        let destination_folder_name = format!("/tmp/tmp_dst_{}", rand::random::<u32>());

        Graph::<T>::from_adjacency_list(
            edge_list
                .iter()
                .map(|(src, dst)| Ok((src.clone(), dst.clone()))),
            destination_folder_name.as_str(),
        )
        .unwrap()
    }

    #[test]
    fn basic_graph_traversal() {
        let graph = get_basic_graph();

        let mut compute = ComputeGraph::<u32, u32>::new(&graph);

        // Initialize graph
        // All nodes on, and data is 0.5
        for id in 0..graph.n_nodes() {
            compute.set_active(id, true);
            compute.set_data(id, 0);
        }
        compute.step(); // Set data

        compute.push(|_, new_res| *new_res += 1);
        compute.step();

        assert_eq!(compute.get_data_as_slice(), &vec![0, 1, 2, 0, 0, 1, 0, 1]);
    }

    #[test]
    fn filtered_graph_traversal() {
        let graph = get_basic_graph();

        let mut compute = ComputeGraph::<u32, u32>::new(&graph);

        // Initialize graph
        // Even nodes are on, and data is 0
        for id in 0..graph.n_nodes() {
            compute.set_active(id, id % 2 == 0);
            compute.set_data(id, 0);
        }
        compute.step(); // Set data

        // Iterate graph once
        compute.push(|_, new_res| *new_res += 1);
        compute.step();

        assert_eq!(compute.get_data_as_slice(), &vec![0, 1, 1, 0, 0, 0, 0, 1]);
    }

    #[test]
    fn bfs_disconnected() {
        let graph = get_basic_graph();

        let mut compute = ComputeGraph::<u32, u32>::new(&graph);

        // Initialize the graph
        // All nodes are off, and data is u32::MAX
        for id in 0..graph.n_nodes() {
            compute.set_active(id, false);
            compute.set_data(id, u32::MAX);
        }

        // Initialize source
        compute.set_active(0, true);
        compute.set_data(0, 0);
        compute.step(); // Set data

        while compute.n_active() > 0 {
            compute.push(|local, res| {
                if local + 1 < *res {
                    *res = local + 1
                }
            });
            compute.step();
        }

        assert_eq!(
            compute.get_data_as_slice(),
            &vec![0, 1, 1, u32::MAX, u32::MAX, 2, u32::MAX, u32::MAX]
        );
    }

    #[test]
    fn bfs_cycle() {
        let edges = vec![
            (0u32, 1u32),
            (1, 2),
            (2, 3),
            (3, 4),
            (4, 5),
            (5, 6),
            (6, 7),
            (7, 0),
        ];
    
        let graph = get_graph(edges);

        let mut compute = ComputeGraph::<u32, u32>::new(&graph);

        // Initialize the graph
        // All nodes are off, and data is u32::MAX
        for id in 0..graph.n_nodes() {
            compute.set_active(id, false);
            compute.set_data(id, u32::MAX);
        }

        // Initialize source
        compute.set_active(0, true);
        compute.set_data(0, 0);
        compute.step(); // Set data

        while compute.n_active() > 0 {
            compute.push(|local, res| {
                if local + 1 < *res {
                    *res = local + 1
                }
            });
            compute.step();
        }

        assert_eq!(
            compute.get_data_as_slice(),
            &vec![0, 1, 2, 3, 4, 5, 6, 7]
        );
        
    }
}

use super::*;

pub trait Algorithm {}

pub trait GraphData: Copy + Default + Sync {}

impl GraphData for u32 {}
impl GraphData for u64 {}
impl GraphData for f32 {}
impl GraphData for f64 {}

pub struct ComputeGraph<'a, T, DataType> {
    graph: &'a Graph<'a, T>,
    old_active: Vec<bool>, // which nodes are active in the old
    new_active: Vec<bool>, // which nodes are active in the new iteration
    data: Vec<DataType>,   // the data associated with each node
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
            data: vec![Default::default(); n_nodes],
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
        self.data[idx] = data;
    }

    /// Performs a global iteration step, useful in many algorithms.
    /// The previous status of all nodes is now updated to the new status.
    /// The new status is reset to false.
    pub fn step(&mut self) {
        // Swap old and new
        std::mem::swap(&mut self.old_active, &mut self.new_active);

        // Set new all to false
        self.new_active.iter_mut().for_each(|x| *x = false);
    }

    /// Returns how many nodes are active in the last iteration.
    /// This function calculates the value every time, so it is recommended to store its value.
    pub fn n_active(&self) -> usize {
        self.old_active.iter().filter(|x| **x).count()
    }

    /// Returns an iterator over the nodes which are active.
    pub fn iter(&mut self) -> impl Iterator<Item = (usize, &[T], &mut bool, &mut DataType)> {
        // (node, edge_list)
        self.graph
            .iter()
            .enumerate()
            .zip(self.old_active.iter_mut())
            .zip(self.data.iter_mut())
            .map(|(((idx, edges), active), data)| (idx, edges, active, data))
            .filter(|(_, _, active, _)| **active)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_basic_graph<'a>() -> Graph<'a, u32> {
        // Default graph
        let edges = vec![(0u32, 1u32), (0, 2), (1, 5), (1, 2), (4, 7)];

        // Generate random filename
        let destination_folder_name = format!("/tmp/tmp_dst_{}", rand::random::<u32>());

        Graph::<u32>::from_adjacency_list(
            edges
                .iter()
                .map(|(src, dst)| Ok((src.clone(), dst.clone()))),
            destination_folder_name.as_str(),
        )
        .unwrap()
    }

    #[test]
    fn basic_graph_traversal() {
        let graph = get_basic_graph();

        let mut compute = ComputeGraph::<u32, f32>::new(&graph);

        // Initialize graph
        // All nodes on, and data is 0.5
        for id in 0..graph.n_nodes() {
            compute.set_active(id, true);
            compute.set_data(id, 0.5);
        }
        compute.step(); // Set data

        // Iterate graph once
        let v = compute
            .iter()
            .map(|(idx, edges, status, value)| (idx, edges, *status, *value))
            .collect::<Vec<_>>();

        assert_eq!(
            v,
            vec![
                (0usize, vec![1u32, 2].as_slice(), true, 0.5),
                (1, vec![5, 2].as_slice(), true, 0.5),
                (2, vec![].as_slice(), true, 0.5),
                (3, vec![].as_slice(), true, 0.5),
                (4, vec![7].as_slice(), true, 0.5),
                (5, vec![].as_slice(), true, 0.5),
                (6, vec![].as_slice(), true, 0.5),
                (7, vec![].as_slice(), true, 0.5),
            ]
        );
    }

    #[test]
    fn filtered_graph_traversal() {
        let graph = get_basic_graph();

        let mut compute = ComputeGraph::<u32, f32>::new(&graph);

        // Initialize graph
        // Even nodes are on, and data is 0.5
        for id in 0..graph.n_nodes() {
            compute.set_active(id, id % 2 == 0);
            compute.set_data(id, 0.5);
        }
        compute.step(); // Set data

        // Iterate graph once
        let v = compute
            .iter()
            .map(|(idx, edges, status, value)| (idx, edges, *status, *value))
            .collect::<Vec<_>>();

        assert_eq!(
            v,
            vec![
                (0usize, vec![1u32, 2].as_slice(), true, 0.5),
                (2, vec![].as_slice(), true, 0.5),
                (4, vec![7].as_slice(), true, 0.5),
                (6, vec![].as_slice(), true, 0.5),
            ]
        );
    }
}

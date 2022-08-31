use super::{
    util::{GraphData, ValidGraphType},
    Graph,
};

use atomic::Atomic;
use rayon::prelude::*;

pub struct ComputeGraph<'a, T, DataType> {
    graph: &'a Graph<'a, T>,
    old_active: Vec<Atomic<bool>>, // which nodes are active in the old
    new_active: Vec<Atomic<bool>>, // which nodes are active in the new iteration
    old_data: Vec<Atomic<DataType>>, // the data of the old iteration
    new_data: Vec<Atomic<DataType>>, // the data of the new iteration
}

impl<'a, T, DataType> ComputeGraph<'a, T, DataType>
where
    T: ValidGraphType + Send + Sync,
    DataType: GraphData,
{
    /// Creates a new graph that can run algorithms and can keep track of node data as well as the active nodes.
    pub fn new(graph: &'a Graph<'a, T>) -> Self {
        let n_nodes = graph.n_nodes();
        Self {
            graph,
            old_active: (0..n_nodes).map(|_| Atomic::new(false)).collect::<Vec<_>>(),
            new_active: (0..n_nodes).map(|_| Atomic::new(false)).collect::<Vec<_>>(),
            old_data: (0..n_nodes)
                .map(|_| Atomic::new(DataType::default()))
                .collect::<Vec<_>>(),
            new_data: (0..n_nodes)
                .map(|_| Atomic::new(DataType::default()))
                .collect::<Vec<_>>(),
        }
    }

    /// Set a single node as active status.
    #[inline]
    pub fn set_active(&mut self, idx: usize, status: bool) {
        self.new_active[idx].store(status, atomic::Ordering::Release);
    }

    /// Sets a single node's data.
    #[inline]
    pub fn set_data(&mut self, idx: usize, data: DataType) {
        self.new_data[idx].store(data, atomic::Ordering::Release);
    }

    /// Performs a global iteration step, useful in many algorithms.
    /// The previous status of all nodes is now updated to the new status.
    /// The new status is reset to false.
    pub fn step(&mut self) {
        // Swap old and new
        std::mem::swap(&mut self.old_active, &mut self.new_active);
        std::mem::swap(&mut self.old_data, &mut self.new_data);

        // Set new all to false
        self.new_active
            .par_iter_mut()
            .for_each(|x| x.store(false, atomic::Ordering::Release));

        self.new_data
            .par_iter_mut()
            .zip(self.old_data.par_iter())
            .for_each(|(x, y)| {
                x.store(y.load(atomic::Ordering::Acquire), atomic::Ordering::Release)
            });
    }

    /// Returns how many nodes are active in the last iteration.
    /// This function calculates the value every time, so it is recommended to store its value.
    pub fn n_active(&self) -> usize {
        self.old_active
            .par_iter()
            .filter(|x| x.load(atomic::Ordering::Acquire))
            .count()
    }

    pub fn push<F>(&mut self, func: F)
    where
        F: Fn(&Atomic<DataType>, &Atomic<DataType>) -> bool + Sync,
    {
        self.graph
            .iter()
            .zip(self.old_active.iter())
            .zip(self.old_data.iter())
            .filter(|((_, active), _)| active.load(atomic::Ordering::Acquire))
            .map(|((edges, _), data)| (edges, data))
            .par_bridge()
            .for_each(|(edges, local_data)| {
                for edge in edges {
                    // Update the data
                    if func(&local_data, &self.new_data[edge.as_()]) {
                        self.new_active[edge.as_()].store(true, atomic::Ordering::Release);
                    }
                }
            })
    }

    pub fn get_data_as_slice(&self) -> &[Atomic<DataType>] {
        &self.old_data
    }
}

/// Helper functions for easier atomics.
pub mod helper {
    use super::*;

    /// Performs an atomic min function by using atomic operations
    pub fn atomic_min<T, F>(src: &Atomic<T>, dst: &Atomic<T>, value: F) -> bool
    where
        F: Fn(T) -> T,
        T: Copy + std::cmp::PartialOrd + std::fmt::Debug,
    {
        let src_val = src.load(atomic::Ordering::Acquire);
        let mut dst_val = dst.load(atomic::Ordering::Acquire);
        let mut status = false;

        while value(src_val) < dst_val && !status {
            let res = dst.compare_exchange(
                dst_val,
                value(src_val),
                atomic::Ordering::Release,
                atomic::Ordering::Relaxed,
            );

            match res {
                Ok(_) => status = true,
                Err(val) => dst_val = val,
            }
        }
        status
    }
}

#[cfg(test)]
mod tests {
    use crate::compute::helper::atomic_min;

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

        compute.push(|_, new_res| {
            new_res.fetch_add(1, atomic::Ordering::Relaxed);
            true
        });
        compute.step();

        assert_eq!(
            &compute
                .get_data_as_slice()
                .iter()
                .map(|x| x.load(atomic::Ordering::Acquire))
                .collect::<Vec<_>>(),
            &vec![0, 1, 2, 0, 0, 1, 0, 1]
        );
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
        compute.push(|_, new_res| {
            new_res.fetch_add(1, atomic::Ordering::Relaxed);
            true
        });
        compute.step();

        assert_eq!(
            &compute
                .get_data_as_slice()
                .iter()
                .map(|x| x.load(atomic::Ordering::Acquire))
                .collect::<Vec<_>>(),
            &vec![0, 1, 1, 0, 0, 0, 0, 1]
        );
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
            compute.push(|local, res| atomic_min(local, res, |v| v + 1));
            compute.step();
        }

        assert_eq!(
            &compute
                .get_data_as_slice()
                .iter()
                .map(|x| x.load(atomic::Ordering::Acquire))
                .collect::<Vec<_>>(),
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
            compute.push(|local, res| atomic_min(local, res, |v| v + 1));
            compute.step();
        }

        assert_eq!(
            &compute
                .get_data_as_slice()
                .iter()
                .map(|x| x.load(atomic::Ordering::Acquire))
                .collect::<Vec<_>>(),
            &vec![0, 1, 2, 3, 4, 5, 6, 7]
        );
    }

    #[test]
    fn wcc() {
        let graph = get_basic_graph();

        let mut compute = ComputeGraph::<u32, u32>::new(&graph);

        // Initialize the graph
        // All nodes are ON, and data is the node id
        for id in 0..graph.n_nodes() {
            compute.set_active(id, true);
            compute.set_data(id, id as u32);
        }
        compute.step();

        while compute.n_active() > 0 {
            compute.push(|local, res| atomic_min(local, res, |v| v));
            compute.step();
        }

        assert_eq!(
            &compute
                .get_data_as_slice()
                .par_iter()
                .map(|x| x.load(atomic::Ordering::Acquire))
                .collect::<Vec<_>>(),
            &vec![0, 0, 0, 3, 4, 0, 6, 4]
        );
    }
}

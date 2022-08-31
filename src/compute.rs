use super::{
    util::{GraphData, ValidGraphType},
    Graph,
};

use atomic::Atomic;
use rayon::prelude::*;

/// This is the compute abstraction over a graph.
/// It contains an underlying representation of the data that can support running algorithms over it.
/// Each node contains `DataType` data, and a status indicating whether or not it is active in the next iteration.
/// Its methods are parallelized using atomics, and will yield good multi-threaded performance.
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

    /// Set a single node's activity in the next iteration as `status`.
    #[inline]
    pub fn set_active(&mut self, idx: usize, status: bool) {
        self.new_active[idx].store(status, atomic::Ordering::Relaxed);
    }

    /// Sets a single node's data in the next iteration as `data`.
    #[inline]
    pub fn set_data(&mut self, idx: usize, data: DataType) {
        self.new_data[idx].store(data, atomic::Ordering::Relaxed);
    }

    /// Sets all nodes' activity in the next iteration as `status`.
    #[inline]
    pub fn fill_active(&mut self, status: bool) {
        self.new_active
            .par_iter_mut()
            .for_each(|a| a.store(status, atomic::Ordering::Relaxed));
    }

    /// Sets all nodes' data in the next iteration as `data`.
    #[inline]
    pub fn fill_data(&mut self, data: DataType) {
        self.new_data
            .par_iter_mut()
            .for_each(|a| a.store(data, atomic::Ordering::Relaxed));
    }

    /// Performs a global iteration step, useful in many algorithms.
    /// The previous status of all nodes is now updated to the new status.
    /// The new status is reset to false.
    pub fn step(&mut self) {
        // Swap old and new
        std::mem::swap(&mut self.old_active, &mut self.new_active);
        std::mem::swap(&mut self.old_data, &mut self.new_data);

        // Reset new
        self.fill_active(false);

        // Set new to the status of old
        self.new_data
            .par_iter_mut()
            .zip(self.old_data.par_iter())
            .for_each(|(x, y)| {
                x.store(y.load(atomic::Ordering::Relaxed), atomic::Ordering::Relaxed)
            });
    }

    /// Returns how many nodes are active in the last iteration.
    /// This function calculates the value every time, so it is recommended to store its value.
    pub fn n_active(&self) -> usize {
        self.old_active
            .par_iter()
            .filter(|x| x.load(atomic::Ordering::Relaxed))
            .count()
    }

    /// This function iterates over the active nodes in the last iteration and applies `func` on them.
    /// `func` receives two arguments: `old`, which is the last state of the node, and `new`, which is the current state.
    pub fn push<F>(&mut self, func: F)
    where
        F: Fn(DataType, &Atomic<DataType>) -> bool + Sync,
    {
        self.graph
            .par_iter()
            // Compute only those that are active in the last iteration
            .filter(|(idx, _)| self.old_active[*idx].load(atomic::Ordering::Relaxed))
            .for_each(|(idx, edges)| {
                // Update
                for edge in edges {
                    // If update yielded improvement then
                    if func(
                        self.old_data[idx].load(atomic::Ordering::Relaxed),
                        &self.new_data[edge.as_()],
                    ) {
                        // Mark it as active in the next iteration
                        self.new_active[edge.as_()].store(true, atomic::Ordering::Relaxed);
                    }
                }
            });
    }

    pub fn get_data_as_slice(&self) -> &[Atomic<DataType>] {
        &self.old_data
    }

    /// Saves the computation's data to the specified file in binary format, following the local machine's endianness.
    pub fn save_data_to_file(&self, filename: &str) -> std::io::Result<()> {
        let mut writer = std::io::BufWriter::new(std::fs::File::create(filename).unwrap());
        for data in self.old_data.iter() {
            let value = data.load(atomic::Ordering::Relaxed);
            value.write_self(&mut writer)?;
        }

        Ok(())
    }
}

/// Helper functions for easier atomics.
pub mod helper {
    use super::*;

    /// Performs an atomic min function by using atomic operations
    pub fn atomic_min<T, F>(src_val: T, dst: &Atomic<T>, value: F) -> bool
    where
        F: Fn(T) -> T,
        T: Copy + std::cmp::PartialOrd + std::fmt::Debug,
    {
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
    use byteorder::{NativeEndian, ReadBytesExt};

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
        // All nodes on, and data is 0
        compute.fill_active(true);
        compute.fill_data(0);
        compute.step();

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
        }
        compute.fill_data(0);
        compute.step();

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
        compute.fill_active(false);
        compute.fill_data(u32::MAX);

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
        compute.fill_active(false);
        compute.fill_data(u32::MAX);

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
        compute.fill_active(true);
        for id in 0..graph.n_nodes() {
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

    #[test]
    fn save_file() {
        let graph = get_basic_graph();
        let mut compute = ComputeGraph::<u32, u32>::new(&graph);

        for id in 0..graph.n_nodes() {
            compute.set_data(id, id as u32);
        }
        compute.step();

        let output = format!("/tmp/output_{}", rand::random::<u32>());

        compute.save_data_to_file(&output).unwrap();

        let mut rdr = std::io::BufReader::new(std::fs::File::open(&output).unwrap());

        for i in 0..graph.n_nodes() {
            assert_eq!(i as u32, rdr.read_u32::<NativeEndian>().unwrap());
        }
    }
}

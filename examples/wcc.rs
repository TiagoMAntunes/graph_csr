fn main() {
    let input_graph = {
        let args = std::env::args().skip(1).collect::<Vec<_>>();

        if args.len() != 2 {
            println!("Usage: bfs <converted graph directory> <src_node>");
            return;
        }

        args[0].clone()
    };

    let graph = graph_csr::Graph::<u32>::load_graph(&input_graph).unwrap();

    let mut compute_graph = graph_csr::compute::ComputeGraph::<u32, u32>::new(&graph);

    // Initialize
    for i in 0..graph.n_nodes() {
        compute_graph.set_active(i, true);
        compute_graph.set_data(i, i as u32);
    }
    compute_graph.step(); // Set data

    let mut i = 0;
    while compute_graph.n_active() > 0 {
        let time_start = std::time::Instant::now();
        compute_graph.push(|src, dst| graph_csr::compute::helper::atomic_min(src, dst, |v| v));
        let time_end = std::time::Instant::now();
        compute_graph.step();

        i += 1;
        println!(
            "Iteration {} took {}ms",
            i,
            (time_end - time_start).as_millis()
        );
    }

    // Print results
    print!("[ ");
    for (idx, data) in compute_graph
        .get_data_as_slice()
        .iter()
        .enumerate()
        .take(30)
    {
        print!(
            "{}:{}, ",
            idx,
            data.load(std::sync::atomic::Ordering::Relaxed)
        );
    }
    println!("]");
}

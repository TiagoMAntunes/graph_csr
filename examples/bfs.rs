fn main() {
    let (input_graph, src_node, output_file) = {
        let args = std::env::args().skip(1).collect::<Vec<_>>();

        if args.len() != 2 && args.len() != 3 {
            println!("Usage: bfs <converted graph directory> <src_node> [output_file]");
            return;
        }

        let output = {
            if args.len() == 3 {
                Some(args[2].clone())
            } else {
                None
            }
        };

        (args[0].clone(), args[1].parse::<usize>().unwrap(), output)
    };

    let graph = graph_csr::Graph::<u32>::load_graph(&input_graph).unwrap();
    let mut compute_graph = graph_csr::compute::ComputeGraph::<u32, u32>::new(&graph);

    // Initialize nodes
    compute_graph.fill_active(false);
    compute_graph.fill_data(u32::MAX);

    // Initialize source
    compute_graph.set_active(src_node, true);
    compute_graph.set_data(src_node, 0);

    compute_graph.step(); // Set data

    let mut i = 0;
    while compute_graph.n_active() > 0 {
        let time_start = std::time::Instant::now();

        compute_graph.push(|src, dst| graph_csr::compute::helper::atomic_min(src, dst, |v| v + 1));
        compute_graph.step();

        let time_end = std::time::Instant::now();

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

    // Save file
    if let Some(output_file) = output_file {
        println!("Saving file to {}", output_file);
        compute_graph.save_data_to_file(&output_file).unwrap();
    }
}

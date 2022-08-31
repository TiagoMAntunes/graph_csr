fn main() {
    let (input_graph, output_file) = {
        let args = std::env::args().skip(1).collect::<Vec<_>>();

        if args.len() != 1 && args.len() != 2 {
            println!("Usage: wcc <converted graph directory> [output_file]");
            return;
        }

        let output = {
            if args.len() == 2 {
                Some(args[1].clone())
            } else {
                None
            }
        };

        (args[0].clone(), output)
    };

    let graph = graph_csr::Graph::<u32>::load_graph(&input_graph).unwrap();

    let mut compute_graph = graph_csr::compute::ComputeGraph::<u32, u32>::new(&graph);

    // Initialize
    compute_graph.fill_active(true);
    for i in 0..graph.n_nodes() {
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

    // Save file
    if let Some(output_file) = output_file {
        println!("Saving file to {}", output_file);
        compute_graph.save_data_to_file(&output_file).unwrap();
    }
}

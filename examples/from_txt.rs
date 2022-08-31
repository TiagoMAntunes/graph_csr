fn main() {
    let (input_graph, output_directory) = {
        let args = std::env::args().skip(1).collect::<Vec<_>>();

        if args.len() != 2 {
            println!("Usage: bfs <converted graph directory> <output directory>");
            return;
        }

        (args[0].clone(), args[1].clone())
    };

    let source_file = std::fs::File::open(input_graph).unwrap();

    graph_csr::Graph::<u32>::from_txt_adjacency_list(source_file, &output_directory).unwrap();
}

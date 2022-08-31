# graph_csr

This crate has the objective of serving as the baseline for powerful graph algorithms built in Rust.
It leverages memory-maps by using the [easy_mmap](https://crates.io/crates/easy_mmap) crate, and thus can easily manipulate graphs that exceed the available system memory.

## Usage

At this point, `graph_csr` supports reading directly from binary and txt files. However, such files require to be sorted by source, due to the nature of the CSR graph.
Let us exemplify with a simple example file `graph.txt`:
```
0 1
0 2
1 5
1 2
4 7
```

This graph contains 8 nodes (0 - 7) and 5 edges.

```
use graph_csr;

fn main() {
    let filename = "./graph.txt";
    let output_folder = "./output";
    let file = std::fs::File::open(filename).unwrap();

    let graph = graph_csr::Graph::<u32>::from_txt_adjacency_list(file, output_folder).unwrap();

    for (node, edges) in graph.iter().enumerate() {
        println!("{:?} -> {:?}", node, edges);
    }
}
```

And we can see the following output:
```
0 -> [1, 2]
1 -> [5, 2]
2 -> []
3 -> []
4 -> [7]
5 -> []
6 -> []
7 -> []
```

You can now inspect the folder `output`:
```
> ls output
edge.csr  vertex.csr
```

The graph is now saved for future use, and there is no need of parsing it again (as it can be time consuming).

For examples on how to compute algorithms, check the [examples](examples/) folder. 
You can use [a default parser from txt](examples/from_txt.rs) to parse your SORTED graph.
If you're looking for an example graph, kindly check the [LiveJournal dataset](https://snap.stanford.edu/data/soc-LiveJournal1.html) which is already sorted (but you're required to remove any comment lines from it).
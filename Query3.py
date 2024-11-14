from pyspark import SparkContext

def parse_line(line):
    parts = line.split('\t')
    return (parts[0], parts[1], parts[2])

def filter_relations(entry):
    source, relation, target = entry
    return relation in {'CuG', 'CdG'}   # gene associations

def main(show_compounds=False):
    sc = SparkContext("local", "Proj2Query3")
    
    # load data
    edges_lines = sc.textFile("hetionet/edges.tsv")
    nodes_lines = sc.textFile("hetionet/nodes.tsv")
    
    # ignore headers
    edges_header = edges_lines.first()
    nodes_header = nodes_lines.first()
    
    edges_data = edges_lines.filter(lambda line: line != edges_header).map(parse_line)
    nodes_data = nodes_lines.filter(lambda line: line != nodes_header).map(parse_line)
    
    # filter to get only compound-gene relationships
    compound_gene = edges_data.filter(filter_relations)
    
    # filter nodes to get only genes and create a mapping from ID to name
    gene_id_to_name = nodes_data.filter(lambda x: x[2] == 'Gene').map(lambda x: (x[0], x[1])).collectAsMap()
    
    # map to (gene, compound) and remove duplicates
    gene_compound_pairs = compound_gene.map(lambda x: (x[2], x[0])).distinct()
    
    # map to (gene, 1) and count the number of compounds per gene
    gene_compound_counts = gene_compound_pairs.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
    
    # map to (count, gene) and group genes by compound count
    compound_count_genes = gene_compound_counts.map(lambda x: (x[1], x[0])).groupByKey().mapValues(list)
    
    # sort by key (compound count) in descending order and take top 5
    top_results = compound_count_genes.sortByKey(False).take(5)
    
    print("Query 3 Results")
    for count, genes in top_results:
        for gene in genes:
            print(f"{gene} associated with {count} compounds")
            gene_name = gene_id_to_name.get(gene, "Unknown Gene")
            print(f"  - gene name: {gene_name}")
            if show_compounds:
                # get compounds associated with the gene
                compounds = gene_compound_pairs.filter(lambda x: x[0] == gene).map(lambda x: x[1]).collect()
                print(f"    Compounds: {', '.join(compounds)}")
        
    sc.stop()

if __name__ == "__main__":
    main()
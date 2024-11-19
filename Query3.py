from pyspark import SparkContext

def parse_line(line):
    parts = line.split('\t')
    return (parts[0].strip(), parts[1].strip(), parts[2].strip())

def filter_relations(entry):
    source, relation, target = entry
    return relation in {'CuG', 'CdG', 'CbG'}  # compound-gene associations

def main():
    sc = SparkContext("local", "Proj2Query3")
    
    # load the data
    edges_lines = sc.textFile("hetionet/edges.tsv")
    nodes_lines = sc.textFile("hetionet/nodes.tsv")
    
    # ignore headers
    edges_header = edges_lines.first()
    nodes_header = nodes_lines.first()
    
    edges_data = edges_lines.filter(lambda line: line != edges_header).map(parse_line)
    nodes_data = nodes_lines.filter(lambda line: line != nodes_header).map(parse_line)
    
    # filter to get only compound-gene relationships
    compound_gene = edges_data.filter(filter_relations)
    
    # create a mapping from compound ID to name
    compound_id_to_name = nodes_data.filter(lambda x: x[2] == 'Compound').map(lambda x: (x[0], x[1])).collectAsMap()
    
    # map to (compound, gene) and remove duplicates
    compound_gene_pairs = compound_gene.map(lambda x: (x[0], x[2])).distinct()
    
    # group genes by compound and count unique genes for each compound
    compound_gene_counts = (
        compound_gene_pairs
        .groupByKey()
        .mapValues(lambda genes: len(set(genes)))
    )
    
    # sort by number of genes in descending order and take the top 5 compounds
    top_compounds = compound_gene_counts.sortBy(lambda x: x[1], ascending=False).take(5)
    
    print("Query 3 Results")
    for compound, gene_count in top_compounds:
        compound_name = compound_id_to_name.get(compound, "Unknown Compound")
        print(f"{compound_name} ({compound}) is associated with {gene_count} unique genes")
    
    sc.stop()

if __name__ == "__main__":
    main()
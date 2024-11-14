from pyspark import SparkContext

def parse_line(line):
    parts = line.split('\t')
    return (parts[0], parts[1], parts[2])

def filter_compound_relations(entry):
    _, relation, _ = entry
    return relation.startswith('C')  # focus on relations starting with 'C' indicating compound interactions

def main():
    sc = SparkContext("local", "Proj2Query1")
    
    # load the data from the TSV file
    lines = sc.textFile("hetionet/edges.tsv")
    
    # skip the header and parse each line
    header = lines.first()
    data = lines.filter(lambda line: line != header).map(parse_line)
    
    # filter and map relationships
    compound_to_gene = data.filter(lambda x: 'G' in x[1] and 'C' == x[0][:1]).map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
    compound_to_disease = data.filter(lambda x: 'D' in x[1] and 'C' == x[0][:1]).map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)

    # join the two datasets on the compound key
    compound_associations = compound_to_gene.join(compound_to_disease)

    # sort by number of genes in descending order and take top 5
    top_results = compound_associations.sortBy(lambda x: x[1][0], ascending=False).take(5)
    
    print("Query 1 Results")
    for compound, (gene_count, disease_count) in top_results:
        print(f"Compound: {compound}\n\t- {gene_count} genes {disease_count} diseases associated")
        
    sc.stop()

if __name__ == "__main__":
    main()
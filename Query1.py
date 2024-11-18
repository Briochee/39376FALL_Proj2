from pyspark import SparkContext

def parse_line(line):
    parts = line.split('\t')
    return (parts[0], parts[1], parts[2])

def filter_compound_to_gene(entry):
    _, relation, _ = entry
    return relation in {"CuG", "CdG", "CbG"}  # specific compound-to-gene relationships

def filter_compound_to_disease(entry):
    _, relation, _ = entry
    return relation in {"CtD", "CpD"}  # specific compound-to-disease relationships

def main():
    sc = SparkContext("local", "Proj2Query1")
    
    # load the data from the TSV file
    lines = sc.textFile("hetionet/edges.tsv")
    
    # skip the header and parse each line
    header = lines.first()
    data = lines.filter(lambda line: line != header).map(parse_line)
    
    # filter compound-to-gene relationships and remove duplicates
    compound_to_gene = (
        data.filter(filter_compound_to_gene)
        .map(lambda x: (x[0], x[2]))  # map to compound and gene
        .distinct()
        .map(lambda x: (x[0], 1))
        .reduceByKey(lambda a, b: a + b)  # count unique genes per compound
    )
    
    # filter compound-to-disease relationships
    compound_to_disease = (
        data.filter(filter_compound_to_disease)
        .map(lambda x: (x[0], x[2]))  # map to compound and disease
        .distinct()
        .map(lambda x: (x[0], 1))
        .reduceByKey(lambda a, b: a + b)  # count unique diseases per compound
    )

    # join the two datasets on the compound key
    compound_associations = compound_to_gene.join(compound_to_disease)

    # sort by the number of genes in descending order and take the top 5
    top_results = compound_associations.sortBy(lambda x: x[1][0], ascending=False).take(5)
    
    print("Query 1 Results")
    for compound, (gene_count, disease_count) in top_results:
        print(f"Compound: {compound}\n\t- {gene_count} unique genes and {disease_count} diseases associated")
        
    sc.stop()

if __name__ == "__main__":
    main()
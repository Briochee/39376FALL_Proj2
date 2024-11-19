from pyspark import SparkContext

def parse_line(line):
    parts = line.strip().split('\t')
    return (parts[0].strip(), parts[1].strip(), parts[2].strip())

def filter_compound_to_gene(entry):
    source, relation, target = entry
    return relation in {"CuG", "CdG", "CbG"} and source.startswith("Compound::") and target.startswith("Gene::")

def filter_compound_to_disease(entry):
    source, relation, target = entry
    return relation in {"CtD", "CpD"} and source.startswith("Compound::") and target.startswith("Disease::")

def main():
    sc = SparkContext("local", "Proj2Query1")
    
    # load the data
    lines = sc.textFile("hetionet/edges.tsv")
    
    # skip the header and parse each line
    header = lines.first()
    data = lines.filter(lambda line: line != header).map(parse_line)
    
    # filter compound-to-gene relationships and count unique genes per compound
    compound_to_gene = (
        data.filter(filter_compound_to_gene)
        .map(lambda x: (x[0], x[2]))  # map to compound and gene
        .distinct()
        .map(lambda x: (x[0], 1))    # map to (compound, 1)
        .reduceByKey(lambda a, b: a + b)  # count unique genes per compound
    )
    
    # get top 5 compounds by the number of associated genes
    top_compounds_by_genes = compound_to_gene.sortBy(lambda x: x[1], ascending=False).take(5)
    
    # filter compound-to-disease relationships
    compound_to_disease = (
        data.filter(filter_compound_to_disease)
        .map(lambda x: (x[0], x[2]))  # map to compound and disease
        .distinct()
    )
    
    # collect compound-to-disease mappings as a dictionary for quick lookup
    compound_to_disease_map = compound_to_disease.groupByKey().mapValues(len).collectAsMap()

    print("Query 1 Results")
    for compound, gene_count in top_compounds_by_genes:
        disease_count = compound_to_disease_map.get(compound, 0)
        print(f"Compound: {compound}\n\t- {gene_count} genes and {disease_count} diseases")
        
    sc.stop()

if __name__ == "__main__":
    main()
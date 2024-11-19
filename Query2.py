from pyspark import SparkContext

def parse_line(line):
    parts = line.split('\t')
    return (parts[0].strip(), parts[1].strip(), parts[2].strip())

def filter_relations(entry):
    source, relation, target = entry
    return relation in {'CtD', 'CpD'}

def main(): 
    sc = SparkContext("local", "Proj2Query2")
    
    # load the data
    edges_lines = sc.textFile("hetionet/edges.tsv")
    
    edges_header = edges_lines.first()
    edges_data = edges_lines.filter(lambda line: line != edges_header).map(parse_line)
    
    # filter compound-disease relationships
    compound_disease_relations = edges_data.filter(filter_relations)
    
    # group by target (diseases) and count the number of unique sources (drugs) for each disease
    disease_to_drugs = (
        compound_disease_relations
        .map(lambda x: (x[2], x[0]))
        .distinct()
        .groupByKey()
        .mapValues(len)
    )
    
    # count diseases by the number of drugs associated
    num_drugs_to_disease_count = (
        disease_to_drugs
        .map(lambda x: (x[1], 1))
        .reduceByKey(lambda a, b: a + b)
    )
    
    # sort by the number of diseases in descending order
    top_disease_counts = (
        num_drugs_to_disease_count
        .sortBy(lambda x: -x[1])
        .take(5)
    )
    
    print("Query 2 Results")
    for count, num_diseases in top_disease_counts:
        print(f"{count} compounds associated with {num_diseases} diseases")
    
    sc.stop()

if __name__ == "__main__":
    main()
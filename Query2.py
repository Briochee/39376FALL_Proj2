from pyspark import SparkContext

def parse_line(line):
    parts = line.split('\t')
    return (parts[0], parts[1], parts[2])

def filter_relations(entry):
    source, relation, target = entry
    return relation in {'CtD', 'CpD'}   # compound associations

def main(show_compounds=False): # default set to false to not print compounds associated
    sc = SparkContext("local", "Proj2Query2")
    
    # ignore data
    edges_lines = sc.textFile("hetionet/edges.tsv")
    nodes_lines = sc.textFile("hetionet/nodes.tsv")
    
    # skip headers
    edges_header = edges_lines.first()
    nodes_header = nodes_lines.first()
    
    edges_data = edges_lines.filter(lambda line: line != edges_header).map(parse_line)
    nodes_data = nodes_lines.filter(lambda line: line != nodes_header).map(parse_line)
    
    # filter to get only compound-disease relationships
    compound_disease = edges_data.filter(filter_relations)
    
    # filter nodes to get only diseases and create a mapping from ID to name
    disease_id_to_name = nodes_data.filter(lambda x: x[2] == 'Disease').map(lambda x: (x[0], x[1])).collectAsMap()
    
    # map to (disease, compound) and remove duplicates
    disease_compound_pairs = compound_disease.map(lambda x: (x[2], x[0])).distinct()
    
    # map to (disease, 1) and count the number of compounds per disease
    disease_compound_counts = disease_compound_pairs.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
    
    # map to (count, disease) and group diseases by compound count
    compound_count_diseases = disease_compound_counts.map(lambda x: (x[1], x[0])).groupByKey().mapValues(list)
    
    # sort by key (compound count) in descending order and take top 5
    top_results = compound_count_diseases.sortByKey(False).take(5)
    
    print("Query 2 Results")
    for count, diseases in top_results:
        for disease in diseases:
            print(f"{disease} associated with {count} compounds")
            disease_name = disease_id_to_name.get(disease, "Unknown Disease")
            print(f"  - disease name: {disease_name}")
            if show_compounds:
                # get compounds associated with the disease
                compounds = disease_compound_pairs.filter(lambda x: x[0] == disease).map(lambda x: x[1]).collect()
                print(f"    Compounds: {', '.join(compounds)}")
        
    sc.stop()

if __name__ == "__main__":
    main()
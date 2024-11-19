from pyspark import SparkContext

def parse_line(line):
    parts = line.split('\t')
    return (parts[0].strip(), parts[1].strip(), parts[2].strip())

def filter_relations(entry):
    source, relation, target = entry
    return relation in {'CtD', 'CpD'}  # compound-disease associations

def main(): 
    sc = SparkContext("local", "Proj2Query2")
    
    # load the data
    edges_lines = sc.textFile("hetionet/edges.tsv")
    
    # skip the header and parse each line
    edges_header = edges_lines.first()
    edges_data = edges_lines.filter(lambda line: line != edges_header).map(parse_line)
    
    # filter to get only compound-disease relationships
    compound_disease = edges_data.filter(filter_relations)
    
    # map to (compound, disease) and remove duplicates
    compound_disease_pairs = compound_disease.map(lambda x: (x[0], x[2])).distinct()
    
    # group diseases by compound and count the number of diseases per compound
    compound_disease_counts = (
        compound_disease_pairs
        .groupByKey()
        .mapValues(len)
    )
    
    # reverse to (number of diseases, compound) for sorting
    disease_count_compounds = compound_disease_counts.map(lambda x: (x[1], x[0]))
    
    # count how many compounds have the same number of associated diseases
    count_compounds = (
        disease_count_compounds
        .groupByKey()
        .mapValues(list)
    )
    
    # sort by number of diseases in descending order and take the top 5
    top_results = count_compounds.sortByKey(False).take(5)
    
    print("Query 2 Results")
    for num_diseases, compounds in top_results:
        print(f"{len(compounds)} drugs are associated with {num_diseases} diseases")
        for compound in compounds:
            print(f"    - {compound}")
    
    sc.stop()

if __name__ == "__main__":
    main()
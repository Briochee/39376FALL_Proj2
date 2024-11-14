from pyspark import SparkContext

# initialize a Spark context
sc = SparkContext(appName="Proj2Query1")

# load data into RDDs
nodes_rdd = sc.textFile("hetionet/nodes.tsv")
edges_rdd = sc.textFile("hetionet/edges.tsv")

# parse nodes
def parse_nodes(line):
    parts = line.split("\t")
    return (parts[0], parts[2].strip())  # (node_id, type), ignoring name

# parse edges
def parse_edges(line):
    parts = line.split("\t")
    return (parts[0], parts[2].strip())  # (source_id, target_id), ignoring metaedge

# map nodes to their types
node_types = nodes_rdd.map(parse_nodes).cache()

# node types as a dictionary
node_types_dict = node_types.collectAsMap()
node_types_bc = sc.broadcast(node_types_dict)

# filter to get edges that are from compounds to genes or diseases
def filter_edges(edge):
    source_type = node_types_bc.value.get(edge[0], None)
    target_type = node_types_bc.value.get(edge[1], None)
    return source_type == 'Compound' and (target_type == 'Gene' or target_type == 'Disease')

# map edges to count gene and disease connections
def edge_mapper(edge):
    target_type = node_types_bc.value[edge[1]]
    return (edge[0], (1 if target_type == 'Gene' else 0, 1 if target_type == 'Disease' else 0))

# process edges
relevant_edges = edges_rdd.map(parse_edges).filter(filter_edges)
compound_counts = relevant_edges.map(edge_mapper).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# sort by the number of genes associated, descending
top_compounds = compound_counts.sortBy(lambda x: x[1][0], ascending=False).take(5)

print("Query 1 Results")
for result in top_compounds:
    print("Compound ID: {}, Genes Associated: {}, Diseases Associated: {}".format(result[0], result[1][0], result[1][1]))

# stop the Spark context
sc.stop()
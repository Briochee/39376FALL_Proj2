# 39376FALL_Proj2

– Data: HetIONet (nodes.tsv and edges.tsv)
– Q1: For each compound, compute the number of genes and the number of diseases associated with the compound. Output results with top 5 number of genes in a descending order.
– Q2: Compute the number of diseases associated with 1, 2, 3, …, n drugs. Output results with the top 5 number of diseases in a descending order.
– Q3: Get the name of drugs that have the top 5 number of genes. Out put the results.


    # Compound relations
        "CrC": compound resembles compound
        "CtD": compound treats diseas
        "CpD": compound palliates disease
        "CuG": compound upregulates gene
        "CdG": compound downregulates gene
        "CbG": compound binds gene
    # Disease relations
        "DrD": disease resembles disease
        "DlA": disease localizes anatomy
        "DuG": disease upregulates gene
        "DdG": disease downregulates gene
        "DaG": disease associates gene
    # Anatomy relations
        "AuG": anatomy upregulates gene
        "AdG": anatomy downregulates gene
        "AeG": anatomy expresses gene
    # Gene relations
        "Gr>G": gene regulates gene
        "GcG": gene covaries gene
        "GiG": gene interates gene


Run code with "spark-submit Query1.py >> output/output.txt"
                "spark-submit Query2.py >> output/output.txt"
                "spark-submit Query3.py >> output/output.txt"
# Hadoop-MapReduce
implementation of algorithms with Hadoop MapReduce

## PageRank
![Top 10 PageRank](PageRank/Top10.PNG)

Step 1： Sum contributions of links and then multiply β to solve 'spider traps'。

![Step 1](PageRank/Step1.PNG)

Step 2： Renormalize r to re-insert the leaked PageRank casued by 'dead-ends'。

![Step 2](PageRank/Step2.PNG)

## LSH

## K-means
![Comparison](K-means/Comparison.PNG)
![Comparison_1](K-means/Comparison_1.PNG)

### Figures show the cost of two initialization strategies and two metrics 

c1：randomly initialized centroids

c2：centroids which are as far apart as possible

![K-means_algo](K-means/K-means_algo.PNG)


## Apriori
![result](Apriori/Apriori_result.PNG)

### We set the support threshold as 60 and get four 4-frequent itemsets. Based on these itemsets, associasion rules can be generated.


PamPa-HD
=======
Frequent closed itemset mining is among the most complex exploratory techniques in data mining, and provides the ability to discover hidden correlations in transactional datasets.
The explosion of Big Data is leading to new parallel and distributed approaches. Unfortunately, most of them are designed to cope with low-dimensional datasets, whereas no distributed high-dimensional frequent closed itemset mining algorithms exists. This work introduces PaMPa-HD, a parallel MapReduce-based frequent closed itemset mining algorithm for high-dimensional datasets, based on Carpenter. The experimental results, performed on both real and synthetic datasets, show the efficiency and scalability of PaMPa-HD.

Usage
=======

## Compilation

The algorithm extracts the frequent closed itemsets from dataset transposed in vertical representation. The format is the same of the example file `example_1.txt` in the repository. 
	
	item,tidSPACEtidSPACEtidSPACE... 

	e.g. 10670,7 10 25 26 35 41 47 50 52 54 64 67 79

The project uses maven to compile & build. Following command should download the dependencies and create the required jar file in `target` directory.

	mvn package

## Run the algorithm

After that, the following command runs the algorithm:

	hadoop jar target/target/PampaHD-0.1.0.jar it.polito.dbdmg.pampa_HD.Driver <number_of_reducer> <input_vertical_dataset> <output_directory> <minsup> <expansion_threshold>

The results can be found in the hdfs directory `<output_directory>/FI`.

Please note that the implementation is developed and tested on 2.5.0-cdh5.3.1 Cloudera configuration.


## Example of use:

In the repository, the script `run.sh`, automatically deletes the directory with the same name of the desired output (if exists) and extracts the frequent closed itemsets.

	sh run.sh

In the script is possible to customize the number of reducers, the max_threshold and the minsup values.

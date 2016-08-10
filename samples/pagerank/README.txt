README for PageRank Application
=================================

Pre-Condition:
--------------
Assume you have already copy the Twister-Pagerank-${Release}.jar into "apps" directory.

Generating Data:
----------------
./gen_data.sh [input file prefix][num splits=num maps][num urls]

e.g. ./gen_data.sh data/data_ 8 1600

Distributing Data:
------------------

To distribute the generated data files you can use the twister.sh utility available in $TWISTER_HOM/bin directory as follows.

./twister.sh put [input data directory (local)][destination directory (remote)]
destination directory - relative to data_dir specified in twister.properties

e.g. ./twister.sh put ../samples/pagerank/bin/data/ /pagerank

Here /pagerank is the relative path of the sub directory that is available inside the data_dir of all compute nodes. You can use ./twister.sh mkdir to create any sub directory inside data_dir.

Create Partition File:
----------------------
Irrespective of whether you distributed data using above method or manually you need to create a partition file to run the application. Please run the following script in $TWISTER_HOM/bin directory as follows.

./create_partition_file.sh [common directory][file filter][partition file]

e.g. ./create_partition_file.sh /pagerank data_ pagerank.pf

Run PageRank:
---------------

Once the above steps are successful you can simply run the following shell script to run PageRank appliction.

./run_pagerank.sh [num urls][num map tasks][num reduce tasks][partition file][output file]                    

e.g. ./run_pagerank.sh 1600 8 1 partition.pf pagerank.txt 

During the processing, you will see the difference in each iteration. When the difference is less than a 
predefined threshold,the Marcov chain converge,the computation stops,you can find the final PageRank values 
in the output file.                           





README for Word Count Application
=================================

Pre-Condition:
--------------
Assume you have already copy the Twister-WordCount-${Release}.jar into "apps" directory.

Generating Data:
----------------
./gen_data.sh [file prefix][directory][totalSize in GB][number of files]

e.g. ./gen_data.sh wc_data data 2 256

Here data refers to the directory where you want the data fiels to be saved.

Distributing Data:
------------------

To distribute the generated data files you can use the twister.sh utility available in $TWISTER_HOM/bin directory as follows.

./twister.sh put [input data directory (local)][destination directory (remote)]
destination directory - relative to data_dir specified in twister.properties

e.g. ./twister.sh put ../samples/wordcount/bin/data/ /wc

Here /wc is the relative path of the sub directory that is available inside the data_dir of all compute nodes. You can use ./twister.sh mkdir to create any sub directory inside data_dir.

Create Partition File:
----------------------
Irrespective of whether you distributed data using above method or manually you need to create a partition file to run the application. Please run the following script in $TWISTER_HOM/bin directory as follows.

./create_partition_file.sh [common directory][file filter][partition file]

e.g. ./create_partition_file.sh /wc wc_data wc.pf

Run Word Count:
---------------

Once the above steps are successful you can simply run the following shell script to run word count application.

./run_wc.sh  [partition File][output file][num maps][num reducers]

e.g. ./run_wc.sh  wc.pf wc.out 256 8

After a while you should be able to see the words and their counts. As a verification, the word "produce" will have a count similar to the number of map tasks you specify. In this case it will be 256.





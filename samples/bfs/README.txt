README for Breadth-first Graph Search Application
=================================================

Pre-Condition:
--------------
Assume you have already copy the Twister-BFS-${Release}.jar into "apps" directory.

Generating Data:
----------------
./graph_gen.sh [num graph nodes][edges ratio][graph file name] 

High ratio will reduce the number of edges one node can have, that will possibly make the graph unconnected. Low ratio will create a lot of edges, this will make the generation time much longer.

e.g. 
./graph_gen.sh 100000 1000 graph-file
./graph_gen.sh 1000000 10000 graph-file

Graph file will be generated in local dir.


Run Breadth-first Graph Search:
---------------

./graph_search.sh [num maps][num reducers][graph file name]

e.g. ./graph_search.sh 8 4 graph-file 

the output is printed in console, you can redirect it into the file.

e.g. ./graph_search.sh 8 4 graph-file >output

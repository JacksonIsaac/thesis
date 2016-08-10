/*
 * Software License, Version 1.0
 *
 *  Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 *
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1) All redistributions of source code must retain the above copyright notice,
 *  the list of authors in the original source code, this list of conditions and
 *  the disclaimer listed in this license;
 * 2) All redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the disclaimer listed in this license in
 *  the documentation and/or other materials provided with the distribution;
 * 3) Any documentation included with all redistributions must include the
 *  following acknowledgement:
 *
 * "This product includes software developed by the Community Grids Lab. For
 *  further information contact the Community Grids Lab at
 *  http://communitygrids.iu.edu/."
 *
 *  Alternatively, this acknowledgement may appear in the software itself, and
 *  wherever such third-party acknowledgments normally appear.
 *
 * 4) The name Indiana University or Community Grids Lab or Twister,
 *  shall not be used to endorse or promote products derived from this software
 *  without prior written permission from Indiana University.  For written
 *  permission, please contact the Advanced Research and Technology Institute
 *  ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 * 5) Products derived from this software may not be called Twister,
 *  nor may Indiana University or Community Grids Lab or Twister appear
 *  in their name, without prior written permission of ARTI.
 *
 *
 *  Indiana University provides no reassurances that the source code provided
 *  does not infringe the patent or any other intellectual property rights of
 *  any other entity.  Indiana University disclaims any liability to any
 *  recipient for claims brought by any other entity based on infringement of
 *  intellectual property rights or otherwise.
 *
 * LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO
 * WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 * NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF
 * INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS.
 * INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS",
 * "VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.
 * LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR
 * ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION
 * GENERATED USING SOFTWARE.
 */

package cgl.imr.samples.bfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.KeyValuePair;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterModel;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.types.BytesValue;
import cgl.imr.types.StringKey;

/**
 * Main class for graph breadth-first search
 * 
 * @author Bingjing Zhang
 * 
 */
public class GraphSearch {

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.out
					.println("Usage: [map task number][reduce task number][graph file name]");
			System.exit(-1);
		}

		int numMapTasks = Integer.parseInt(args[0]);
		int numReduceTasks = Integer.parseInt(args[1]);
		String graphFile = args[2];

		Graph graph = new Graph();
		graph.loadFromFile(graphFile); // load graph from file
		System.out.println("Total vertices: " + graph.getNodes().size());
		graph.setRoot(1); // set the root

		GraphSearch gs = new GraphSearch(graph);

		try {
			double beginTime = System.currentTimeMillis();
			gs.driveMapReduce(numMapTasks, numReduceTasks);
			double endTime = System.currentTimeMillis();
			System.out
					.println("------------------------------------------------------------");
			System.out.println("All shortest paths calculation took "
					+ (endTime - beginTime) / 1000 + " seconds.");
			System.out
					.println("------------------------------------------------------------");
		} catch (Exception e) {
			e.printStackTrace();
		}

		graph.print(); // print the graph
		System.exit(0);
	}

	private Graph graph;

	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	public GraphSearch(Graph g) {
		this.graph = g;
	}

	/**
	 * check if the breadth-first search is finished after every iteration
	 * 
	 * @param nodes
	 * @return
	 */
	private boolean checkCompleteness(HashMap<Integer, Node> nodes) {
		for (Node n : nodes.values()) {
			if (n.getColor() != 2) {
				return false;
			}
		}
		return true;
	}

	/**
	 * main function for map-reduce computation
	 * 
	 * @throws Exception
	 */
	public void driveMapReduce(int numMapTasks, int numReducers)
			throws Exception {
		long beforeTime = System.currentTimeMillis();

		System.out.println("NUM_MAP: " + numMapTasks + " NUM_REDUCE: "
				+ numReducers);

		// JobConfigurations
		JobConf jobConf = new JobConf("graph-bfs-map-reduce"
				+ uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(BFSMapTask.class);
		jobConf.setReducerClass(BFSReduceTask.class);
		jobConf.setCombinerClass(BFSCombiner.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReducers);
		//jobConf.setFaultTolerance();

		TwisterModel mrDriver = new TwisterDriver(jobConf);
		mrDriver.configureMaps();

		int loopCount = 0;
		TwisterMonitor monitor = null;

		boolean complete = false;

		for (loopCount = 0;; loopCount++) {
			monitor = mrDriver.runMapReduce(getKeyValuesForMap(numMapTasks));
			monitor.monitorTillCompletion();

			HashMap<Integer, Node> newGraph = ((BFSCombiner) mrDriver
					.getCurrentCombiner()).getResults();

			complete = checkCompleteness(newGraph);
			if (complete) {
				System.out.println("Breath First Search Complete !");
				break;
			}

			System.out.println(" Loop Count: " + loopCount);
			// reset nodes of the graph
			this.graph.setNodes(newGraph);
		}

		// Print the test statistics
		double timeInSeconds = ((double) (System.currentTimeMillis() - beforeTime)) / 1000;

		System.out.println("Total Time for Graph BFS : " + timeInSeconds);
		System.out.println("Total loop count : " + (loopCount));
		// Close the MRDriver. This will close the broker connections and
		mrDriver.close();
	}

	/**
	 * use graph nodes data, generate key value pair according to the number of
	 * map task, partition nodes into different groups
	 * 
	 * @param numMaps
	 * @return
	 */
	private List<KeyValuePair> getKeyValuesForMap(int numMaps) {
		List<KeyValuePair> keyValues = new ArrayList<KeyValuePair>();
		List<Graph> subgraphs = graph.getSubGraphs(numMaps);

		StringKey key = null;
		BytesValue value = null;

		int keyNo = 0;
		for (Graph g : subgraphs) {
			key = new StringKey("" + keyNo);
			try {
				value = new BytesValue(g.getBytes());
			} catch (SerializationException e) {
				e.printStackTrace();
			}
			keyValues.add(new KeyValuePair(key, value));
			keyNo++;
		}

		return keyValues;
	}
}

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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Generate a graph file with Power Law rule. Graph is presented in Sparse
 * Matrix Representation Every line begins with node number which is followed by
 * all the nodes it can connect to
 * 
 * @author Bingjing Zhang
 */

public class GraphGen {
	/**
	 * function example GraphGen ggen =new GraphGen(100000, 1000);
	 * ggen.generateGraph("graph-file-100000");
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 3) {
			System.out
					.println("Usage: [the number of nodes][edge number ratio][graph file name]");
			System.exit(-1);
		}

		int numNode = Integer.parseInt(args[0]);
		double ratio = Double.parseDouble(args[1]);
		String fileName = args[2];

		GraphGen ggen = new GraphGen(numNode, ratio);
		Graph g = new Graph();
		try {
			ggen.generateGraph(fileName);
			System.out.println("Graph Generated");
			g.loadFromFile(fileName);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		System.out.println("Check Connectivity " + g.checkConnectivity());
	}

	private double edgeratio;
	// value and the max node id.
	private double max; // the max power law value calculated by formula
	private int num; // the number of nodes

	private double seed; // seed represents the multiples between max power law

	public GraphGen(int num, double ratio) {
		this.max = this.calPowerLaw(num);
		this.num = num;
		this.seed = max / num; // seed is used to fix the value of
		// power low into the range of node ids
		this.edgeratio = ratio;
	}

	/**
	 * use power law
	 * 
	 * @param x
	 * @return
	 */
	public double calPowerLaw(int x) {
		return Math.pow(x, 50);
	}

	/**
	 * generate a graph and store it into the file
	 * 
	 * @param filename
	 * @throws IOException
	 */
	public void generateGraph(String filename) throws IOException {
		BufferedWriter writer = null;
		writer = new BufferedWriter(new FileWriter(filename.toString()));

		// for every node, build its edge list
		for (int i = 1; i <= this.num; i++) {
			ArrayList<Integer> edges = new ArrayList<Integer>();
			// calculate the number of linkages by id
			int edgeNum = this.getLinksN(i);
			int edgeCount = edges.size();

			edges.add(i);

			while (edgeCount < edgeNum) {
				// generate non-duplicated edges and output to the file
				int nodeID = (int) (Math.random() * this.num) + 1;

				if (!edges.contains(nodeID)) {
					edges.add(nodeID);
					edgeCount++;
				}
			}

			String value = "";
			for (int v : edges) {
				value = value + " " + v;
			}
			value = value.substring(1);
			value = value + "\n";
			try {
				writer.write(value);
				writer.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		writer.close();
	}

	/**
	 * calculate how many edges can a node have, according to node id.
	 * 
	 * @param x
	 * @return
	 */
	public int getLinksN(int x) {
		double plValue = this.calPowerLaw(x);

		double ratio = this.edgeratio;
		// this ratio is trying to reduce the max number
		// of edges,
		// when the number of nodes is 100000 in size, ratio is better to set
		// 1000
		// when the number of nodes is 1000000 in size, ratio is better to set
		// 10000

		// use 80-20 rules to fix the distribution of the nodes and edges
		// (because power law value often increase too quickly)
		// 80% nodes only have 20% edges, that is 1/16 of the edges of rest 20%
		// nodes
		if (x <= 0.8 * this.num && plValue > this.max / (16 * ratio)) {
			plValue = this.max / (16 * ratio);
		}

		// for the rest 20% nodes, let the value be original power law value
		if (x > 0.8 * this.num && plValue > this.max / ratio) {
			plValue = this.max / ratio;
		}

		// adjust the value to link number, the minimal linkage of one node is
		// 2.
		int links = (int) (plValue / this.seed);
		if (links < 2) {
			links = 2;
		}

		// the max number of linkage
		if (links >= this.num) {
			links = this.num - 1;
		}
		return links;
	}
}

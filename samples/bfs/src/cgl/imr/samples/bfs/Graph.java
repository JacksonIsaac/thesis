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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterSerializable;

/**
 * Class representing a graph data structure.
 * 
 * @author Bingjing Zhang
 * 
 */
public class Graph implements TwisterSerializable {

	private HashMap<Integer, Node> nodes; // use hashmap to store a set of nodes

	public Graph() {
		this.nodes = new HashMap<Integer, Node>();
	}

	/**
	 * create a node, use list to present linked edges
	 * 
	 */
	public void addNode(int id, List<Integer> list) {
		Node node = new Node(id);
		node.setEdges(list);
		this.nodes.put(id, node);
	}

	/**
	 * add a node object into the graph directly
	 * 
	 */
	public void addNode(int id, Node node) {
		this.nodes.put(id, node);
	}

	/**
	 * check connectivity of the graph. used in GraphGen, to make sure the graph
	 * generated is connected similar like serial version of breadth-first
	 * search
	 * 
	 */
	public boolean checkConnectivity() {
		int firstkey = this.nodes.keySet().iterator().next();

		Node rnode = nodes.get(firstkey);
		rnode.setColor(1);
		rnode.setDistance(0);

		Queue<Integer> q = new LinkedList<Integer>();
		q.add(firstkey);

		int count = 1; // include the source
		int maxDis = 0;
		while (!q.isEmpty()) {
			Node unode = this.nodes.get(q.poll());

			for (int v : unode.getEdges()) {
				Node vnode = this.nodes.get(v);
				// if color is white(0), adding into the queue is not needed.
				if (vnode.getColor() == 0) {
					vnode.setColor(1);
					vnode.setDistance(unode.getDistance() + 1);
					if (vnode.getDistance() > maxDis) {
						maxDis = vnode.getDistance();
					}
					q.add(v);
					count++;
				}
			}
			unode.setColor(2);
		}

		boolean conn = false;
		if (count == this.nodes.size()) {
			conn = true;
		}

		// reset graph state
		for (Node n : this.nodes.values()) {
			n.setColor(0);
			n.setDistance(-1);
		}

		System.out.println("MAX DIS " + maxDis);
		return conn;
	}

	@Override
	/**
	 * load object data from bytes
	 */
	public void fromBytes(byte[] bytes) throws SerializationException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);
		int len = 0;
		byte[] data = null;
		int key = 0;
		try {
			int count = din.readInt();
			for (int i = 0; i < count; i++) {
				key = din.readInt();
				len = din.readInt();
				data = new byte[len];
				din.readFully(data);
				this.nodes.put(key, new Node(data));
			}
			din.close();
			baInputStream.close();

		} catch (Exception ioe) {
			throw new SerializationException(ioe);
		}
	}

	@Override
	/**
	 * transfer object data to bytes
	 */
	public byte[] getBytes() throws SerializationException {
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(baOutputStream);
		int count = this.nodes.size();
		byte[] marshalledBytes = null;
		byte[] data;
		try {
			dout.writeInt(count);
			for (int k : this.nodes.keySet()) {
				dout.writeInt(k);
				data = this.nodes.get(k).getBytes();
				dout.writeInt(data.length);
				dout.write(data);
			}
			dout.flush();
			marshalledBytes = baOutputStream.toByteArray();
			baOutputStream = null;
			dout = null;
		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
		return marshalledBytes;
	}

	public Node getNode(int nodeId) {
		return this.nodes.get(nodeId);
	}

	public HashMap<Integer, Node> getNodes() {
		return this.nodes;
	}

	/**
	 * divide a graph into several subgraphs, used in data grouping
	 * 
	 */
	public List<Graph> getSubGraphs(int num) {
		ArrayList<Graph> subgraphs = new ArrayList<Graph>();
		for (int i = 0; i < num; i++) {
			Graph g = new Graph();
			subgraphs.add(g);
		}

		for (int k : this.nodes.keySet()) {
			Node n = this.nodes.get(k);
			Graph g = subgraphs.get(k % num);
			g.addNode(n.getId(), n);
		}
		return subgraphs;
	}

	/**
	 * read a file, load the graph from it
	 * 
	 */
	public void loadFromFile(String filename) {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(filename.toString()));
			while (true) {
				String values = reader.readLine();

				if (values == null) {
					break;
				}

				String[] value = values.split(" ");
				// System.out.println(value[0]);
				Node node = new Node(Integer.parseInt(value[0]));
				ArrayList<Integer> edges = new ArrayList<Integer>();
				for (int i = 1; i < value.length; i++) {
					edges.add(Integer.parseInt(value[i]));
				}
				node.setEdges(edges);
				this.nodes.put(node.getId(), node);	
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * print the whole graph in console
	 * 
	 */
	public void print() {
		for (int v : this.nodes.keySet()) {
			Node vnode = this.nodes.get(v);
			vnode.print();
		}
	}

	public void setNodes(HashMap<Integer, Node> nodes) {
		this.nodes = nodes;
	}

	/**
	 * set the root, where the breadth-first search start
	 * 
	 */
	public void setRoot(int root) {
		// Set the initial conditions for the root node
		Node rnode = this.nodes.get(root);
		rnode.setColor(1);
		rnode.setDistance(0);
	}

	/**
	 * store the graph into the file
	 * 
	 */
	public void storeToFile(String filename) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(filename
				.toString()));
		for (int k : this.nodes.keySet()) {
			Node vnode = this.nodes.get(k);

			String value = "" + vnode.getId();
			for (int v : vnode.getEdges()) {
				value = value + " " + v;
			}
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
}

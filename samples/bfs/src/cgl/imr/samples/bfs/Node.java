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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterSerializable;

/**
 * Class representing a Node in a graph.
 * 
 * @author Bingjing Zhang
 */
public class Node implements TwisterSerializable {

	// white 0, Gray 1, black 2,
	// white means unexplored,
	// Gray means will be explored in next iteration,
	// black means already been explored
	private int color;
	private int distance;
	private List<Integer> edges; // the edges this node can connect to

	private int id; // node id

	private int parent;

	public Node(byte[] bytes) throws SerializationException {
		this(-1);
		fromBytes(bytes);
	}

	public Node(int id) {
		this.id = id;
		this.distance = -1; // distinguish from distance 0, means no distance
		// known yet
		this.parent = 0;
		this.color = 0; // white, not arrived

		this.edges = null; // no edges set at the beginning
	}

	public void addEdge(int value) {
		this.edges.add(value);
	}

	@Override
	/**
	 * translate bytes into node object
	 */
	public void fromBytes(byte[] bytes) throws SerializationException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {
			id = din.readInt();
			distance = din.readInt();
			parent = din.readInt();
			color = din.readInt();

			int count = din.readInt();
			if (count != -1) {
				this.edges = new ArrayList<Integer>();
				for (int i = 0; i < count; i++) {
					edges.add(din.readInt());
				}
			}

			din.close();
			baInputStream.close();
		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
	}

	@Override
	/**
	 * translate object into bytes
	 */
	public byte[] getBytes() throws SerializationException {
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(baOutputStream);
		int count = -1;
		if (this.edges != null) {
			count = this.edges.size();
		}

		byte[] marshalledBytes = null;
		try {
			dout.writeInt(id);
			dout.writeInt(distance);
			dout.writeInt(parent);
			dout.writeInt(color);

			dout.writeInt(count);
			if (count != -1) {
				for (int i = 0; i < count; i++) {
					dout.writeInt(this.edges.get(i));
				}
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

	public int getColor() {
		return color;
	}

	public int getDistance() {
		return this.distance;
	}

	public List<Integer> getEdges() {
		return this.edges;
	}

	public int getId() {
		return this.id;
	}

	public int getParent() {
		return this.parent;
	}

	public void print() {
		System.out.printf("v = %2d parent = %2d distance = %2d ", this.id,
				this.parent, this.distance);
		System.out.print("color =  " + this.color);
		System.out.print(" " + this.edges);
		System.out.print("\n");
	}

	public void setColor(int value) {
		this.color = value;
	}

	public void setDistance(int value) {
		this.distance = value;
	}

	public void setEdges(List<Integer> list) {
		this.edges = list;
	}

	public void setParent(int value) {
		this.parent = value;
	}
}

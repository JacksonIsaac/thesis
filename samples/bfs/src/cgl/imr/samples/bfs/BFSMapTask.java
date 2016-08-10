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

import java.util.LinkedList;
import java.util.Queue;

import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.MapTask;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.types.BytesValue;
import cgl.imr.types.StringKey;

/**
 * MapTask Class
 * 
 * @author Bingjing Zhang
 * 
 */
public class BFSMapTask implements MapTask {

	@Override
	public void close() throws TwisterException {
	}

	@Override
	public void configure(JobConf arg0, MapperConf arg1)
			throws TwisterException {
	}

	@Override
	/**
	 * map function, handle subgraph data, and emit node key-value pair
	 */
	public void map(MapOutputCollector collector, Key key, Value val)
			throws TwisterException {

		Graph subgraph = new Graph();
		try {
			subgraph.fromBytes(val.getBytes());
		} catch (SerializationException e) {
			throw new TwisterException(e);
		}

		Queue<Node> q = new LinkedList<Node>();

		// put the current nodes into the queue
		for (Node node : subgraph.getNodes().values()) {
			if (node.getColor() == 1) {
				q.add(node);
			}
		}

		// breadth-first exploring in serial-like version way
		while (!q.isEmpty()) {
			Node node = q.remove();

			if (node == null) {
				break;
			}

			// null edge nodes need to be merged with subgraph, but not be
			// explored
			// only node with edges be explored here, generate null edge nodes
			if (node.getEdges() != null) {
				for (int v : node.getEdges()) {
					Node n = new Node(v);
					n.setDistance(node.getDistance() + 1);
					n.setColor(1);
					n.setParent(node.getId());
					q.add(n);
				}
				node.setColor(2);
			} else {
				Node n = subgraph.getNode(node.getId());

				// try to merge nodes explored from the queue to the subgraph
				if (n != null) {
					if (node.getColor() > n.getColor()) {
						n.setColor(node.getColor());
					}

					if (n.getDistance() == -1
							|| ((node.getDistance() < n.getDistance()) && (node
									.getDistance() != -1))) {
						n.setDistance(node.getDistance());
						n.setParent(node.getParent());
					}
				} else {
					subgraph.addNode(node.getId(), node);
				}
			}
		}

		for (Node vnode : subgraph.getNodes().values()) {
			try {
				// emit node object,use id as key, node object as value
				collector.collect(new StringKey("" + vnode.getId()),
						new BytesValue(vnode.getBytes()));
			} catch (SerializationException e) {
				throw new TwisterException(e);
			}
		}
	}
}

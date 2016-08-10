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

import java.util.List;

import cgl.imr.base.Key;
import cgl.imr.base.ReduceOutputCollector;
import cgl.imr.base.ReduceTask;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.ReducerConf;
import cgl.imr.types.BytesValue;
import cgl.imr.types.StringKey;

/**
 * ReduceTask Class
 * 
 * @author Bingjing Zhang
 * 
 */
public class BFSReduceTask implements ReduceTask {

	@Override
	public void close() throws TwisterException {
	}

	@Override
	public void configure(JobConf arg0, ReducerConf arg1)
			throws TwisterException {
	}

	@Override
	/**
	 * try to combine nodes with the same id into
	 *  darkest color, shortest distance (set parent together),
	 * edges should not be null.
	 */
	public void reduce(ReduceOutputCollector collector, Key key,
			List<Value> values) throws TwisterException {
		// TODO Auto-generated method stub

		if (values.size() <= 0) {
			System.out.println("REDUCE INPUT ERROR");
			throw new TwisterException("Reduce input error no values.");
		}

		// System.out.println("REDUCE BEGINS");

		int id = -1;
		int color = -1;
		List<Integer> edges = null;
		int distance = -1;
		int parent = -1;

		for (Value value : values) {
			BytesValue val = (BytesValue) value;

			Node node = new Node(0);
			try {
				node.fromBytes(val.getBytes());
			} catch (SerializationException e) {
				throw new TwisterException(e);
			}

			if (id == -1) {
				id = node.getId();
			}

			if (node.getColor() > color) {
				color = node.getColor();
			}

			if (node.getEdges() != null && edges == null) {
				edges = node.getEdges();
			}

			if (node.getDistance() >= 0) {
				if (distance == -1) {
					distance = node.getDistance();
					parent = node.getParent();
				} else if (distance > node.getDistance()) {
					distance = node.getDistance();
					parent = node.getParent();
				}
			}
		}
		Node node = new Node(id);
		node.setColor(color);
		node.setDistance(distance);
		node.setEdges(edges);
		node.setParent(parent);

		try {
			collector.collect(new StringKey("" + node.getId()), new BytesValue(
					node.getBytes()));
		} catch (SerializationException e) {
			throw new TwisterException(e);
		}
	}
}

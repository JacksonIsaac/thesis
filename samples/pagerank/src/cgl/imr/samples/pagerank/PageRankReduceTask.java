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

package cgl.imr.samples.pagerank;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import cgl.imr.base.Key;
import cgl.imr.base.ReduceOutputCollector;
import cgl.imr.base.ReduceTask;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.ReducerConf;
import cgl.imr.types.BytesValue;
import cgl.imr.types.DoubleVectorData;
import cgl.imr.types.IntKey;

/*
 * Reduce task of the page rank algorithm
 *
 * @author Hui Li (lihui@indiana.edu) 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * 
 */

public class PageRankReduceTask implements ReduceTask {
	public void close() throws TwisterException {
		// TODO Auto-generated method stub
	}

	/*
	 * Merge the partial results of local or near by map tasks together, so as
	 * to decrease the network cost
	 * 
	 * @parameter collector -store the intermediate key value pair
	 * 
	 * @parameter key -the index of group of map tasks
	 * 
	 * @parameter values -the list of compressed values of changed urls of
	 * groups
	 */

	public void configure(JobConf jobConf, ReducerConf reducerConf)
			throws TwisterException {
	}

	public void reduce(ReduceOutputCollector collector, Key key, List<Value> values) throws TwisterException {
		try {
			int numUrls, numData, lenData;
			double tanglingProbSum;
			Set<Integer> urlsSet = new HashSet<Integer>();
			BytesValue val = (BytesValue) values.get(0);
			DoubleVectorData tmpDvd = new DoubleVectorData();
			tmpDvd.fromBytes(val.getBytes());
			numData = tmpDvd.getNumData();
			lenData = tmpDvd.getVecLen();

			double[][] tmpPageRank = new double[numData][lenData];
			tmpPageRank = tmpDvd.getData();
			numUrls = (int) (tmpPageRank[0][0]);
			double[][] newPageRank = new double[numUrls][lenData];

			// decompress the compressed partial page rank values and merge them together
			tanglingProbSum = 0.0d;
			int numValues = values.size();
			int index;
			for (int i = 0; i < numValues; i++) {
				val = (BytesValue) values.get(i);
				tmpDvd = new DoubleVectorData();
				tmpDvd.fromBytes(val.getBytes());
				tmpPageRank = tmpDvd.getData();
				numData = tmpDvd.getNumData();
				tanglingProbSum += tmpPageRank[0][1]; // merge the tangling
				// merge the changed page rank values together
				for (int j = 1; j < numData; j++) {
					index = (int) tmpPageRank[j][0];
					newPageRank[index][1] += tmpPageRank[j][1];
					urlsSet.add(index);
				}
			}

			// compress the page rank values by only storing the changed page rank values
			int numChangedUrls = urlsSet.size(); // the number of urls whose rank value changed
					
			int[] urlsArray = new int[numChangedUrls];
			Iterator<Integer> iter = urlsSet.iterator();
			for (int i = 0; i < numChangedUrls; i++) {
				if (iter.hasNext())
					urlsArray[i] = (iter.next()).intValue();
			}
		
			double[][] resultData = new double[numChangedUrls + 1][2];
			resultData[0][0] = numUrls;
			resultData[0][1] = tanglingProbSum;
			for (int i = 1; i <= numChangedUrls; i++) {
				resultData[i][0] = urlsArray[i - 1];
				resultData[i][1] = newPageRank[urlsArray[i - 1]][1];
			}
			DoubleVectorData resultDvd = new DoubleVectorData(resultData,
					numChangedUrls + 1, 2);
			collector.collect(new IntKey(1), new BytesValue(resultDvd.getBytes()));
			// emit the results to combiner
		} catch (SerializationException e) {
			throw new TwisterException(e);
		}
	}
}

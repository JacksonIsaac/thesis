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

import java.io.BufferedWriter;
import java.io.FileWriter;

import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.MapTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.config.TwisterConfigurations;
import cgl.imr.types.IntKey;
import cgl.imr.types.StringValue;

/**
 * Map task for generating adjacency matrix data
 * 
 * @author Hui Li (lihui@indiana.edu)
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * 
 */

public class PageRankDataGenMapTask implements MapTask {

	private JobConf jobConf;

	@Override
	public void close() throws TwisterException {
		// TODO Auto-generated method stub
	}

	/*
	 * Map task for generating the adjacency matrix data of urls
	 * 
	 * @parameter collector - data structure used to store the intermediate key
	 * value pair
	 * 
	 * @parameter key - the index of map task
	 * 
	 * @parameter val - the file name that store adjacency matrix of part page
	 * rank values
	 */

	public void configure(JobConf jobConf, MapperConf mapConf)
			throws TwisterException {
		this.jobConf = jobConf;
	}

	public void map(MapOutputCollector collector, Key key, Value val)
			throws TwisterException {

		long numUrls = Long.parseLong(jobConf
				.getProperty(PageRankDataGen.PROP_NUM_URLS));
		int numMapTasks = Integer.parseInt(jobConf
				.getProperty(PageRankDataGen.PROP_NUM_MAP_TASKS));
		
		String subDataDir = jobConf.getProperty(PageRankDataGen.PROP_DATA_DIR);
		String fileName = ((StringValue) val).toString();
		try {
			fileName = TwisterConfigurations.getInstance().getLocalDataDir()
					+ "/" + subDataDir+"/"+fileName;
			int index = ((IntKey) key).hashCode(); // map task index

			BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
			String strLine = null;
			long i, j;
			long start, end;
			start = index * (numUrls / numMapTasks);
			if (index == numMapTasks - 1)
				end = numUrls;
			else
				end = start + (numUrls / numMapTasks);

			strLine = (end - start) + "\n";
			writer.write(strLine);

			/*
			 * simulate the power law distributions.
			 */
			StringBuffer strBuf = null;
			for (i = start; i < end; i++) {
				strBuf = new StringBuffer();
				strBuf.append(Long.toString(i));
				for (j = 0; j < numUrls; j++) {
					if ((-3*i*i + 7 + i) % (j + 3) == 0)
						strBuf.append(" "+j);
				}// for j;
				strBuf.append("\n");
				writer.write(strBuf.toString());
			}
			writer.flush();
			writer.close();
		} catch (Exception e) {
			throw new TwisterException(e);
		}
	}
}

/**
 * Software License, Version 1.0
 * 
 * Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 * 
 *
 *Redistribution and use in source and binary forms, with or without 
 *modification, are permitted provided that the following conditions are met:
 *
 *1) All redistributions of source code must retain the above copyright notice,
 * the list of authors in the original source code, this list of conditions and
 * the disclaimer listed in this license;
 *2) All redistributions in binary form must reproduce the above copyright 
 * notice, this list of conditions and the disclaimer listed in this license in
 * the documentation and/or other materials provided with the distribution;
 *3) Any documentation included with all redistributions must include the 
 * following acknowledgement:
 *
 *"This product includes software developed by the Community Grids Lab. For 
 * further information contact the Community Grids Lab at 
 * http://communitygrids.iu.edu/."
 *
 * Alternatively, this acknowledgement may appear in the software itself, and 
 * wherever such third-party acknowledgments normally appear.
 * 
 *4) The name Indiana University or Community Grids Lab or Twister, 
 * shall not be used to endorse or promote products derived from this software 
 * without prior written permission from Indiana University.  For written 
 * permission, please contact the Advanced Research and Technology Institute 
 * ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 *5) Products derived from this software may not be called Twister, 
 * nor may Indiana University or Community Grids Lab or Twister appear
 * in their name, without prior written permission of ARTI.
 * 
 *
 * Indiana University provides no reassurances that the source code provided 
 * does not infringe the patent or any other intellectual property rights of 
 * any other entity.  Indiana University disclaims any liability to any 
 * recipient for claims brought by any other entity based on infringement of 
 * intellectual property rights or otherwise.  
 *
 *LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO 
 *WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 *NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF 
 *INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS. 
 *INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS", 
 *"VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.  
 *LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR 
 *ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION 
 *GENERATED USING SOFTWARE.
 */

package cgl.imr.samples.blastalter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import cgl.imr.config.ConfigurationException;
import cgl.imr.config.TwisterConfigurations;
import cgl.imr.samples.blastutil.Query;

public class QueryPartition {

	/**
	 * 	put query lines into query groups. every query group has nearly the same
	 * size of query. If they can not be divided equally, the rest will be
	 * scattered into different group averagely.
	 * 
	 * The program is used in the case that query file is very large (GB level).
	 * Once get a query group, output directly.
	 * The the program won't needs large memory to hold all query line. 
	 * 
	 * @param queryFile
	 * @param noOfSequences
	 * @param numPartition
	 * @param dataDir
	 * @param outputPrefix
	 * @throws Exception
	 */
	private static void outputQueries(String queryFile, int noOfSequences,
			int numPartition, String dataDir, String outputPrefix)
			throws Exception {

		int normalQuantity = noOfSequences / numPartition;
		int maxQuantity = normalQuantity + 1;
		int rest = noOfSequences - normalQuantity * numPartition;

		System.out.println("normalQuantity :" + normalQuantity);
		System.out.println("maxQuantity :" + maxQuantity);
		System.out.println("rest :" + rest);

		BufferedReader reader = new BufferedReader(new FileReader(queryFile));

		Query query = new Query();
		//count group size
		int count = 0;
		//calculate output partition id
		int id = 0;
		for (int i = 0; i < noOfSequences; i++) {
			String descriptionLine = null;
			String dataLine = null;

			if (((descriptionLine = reader.readLine()) == null)
					|| ((dataLine = reader.readLine()) == null)) {
				throw new IOException(
						"Cannot read the sequence from input file.");
			}

			query.addLine(descriptionLine);
			query.addLine(dataLine);
			count++;

			if (rest > 0) {
				if (count == maxQuantity) {
					// since one of the rest is added into the query, subtract
					// it
					rest--;

					query.output(dataDir + outputPrefix + id + ".fa");
					id++;

					// if rest >0, it is not possible to be the last one
					count = 0;
					query = new Query();
				}
			} else {
				// rest == 0
				if (count == normalQuantity) {
					query.output(dataDir + outputPrefix + id + ".fa");
					id++;

					if (i != noOfSequences - 1) {
						count = 0;
						query = new Query();
					}
				}
			}
		}

		reader.close();
	}

	/**
	 * main function
	 * 
	 * @param args
	 */
	public static void main(String args[]) {

		if (args.length != 5) {
			System.err
					.println("args:  [query_file] [sequence_count]  [num_partition] [data_dir] [output_prefix]");
			System.exit(2);
		}

		// load Twister Data Home directory
		TwisterConfigurations tconf = null;
		try {
			tconf = TwisterConfigurations.getInstance();
		} catch (ConfigurationException e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}

		String queryFile = args[0];
		int noOfSequences = Integer.parseInt(args[1]);
		int numPartition = Integer.parseInt(args[2]);

		// working directory for putting temporary input files and final outputs
		// It is relative to Twister_DATA_HOME
		String dataDir = (tconf.getLocalDataDir() + "/" + args[3]+"/").replace(
				"//", "/");
		String outputPrefix = args[4];

		long beforeTime = System.currentTimeMillis();

		try {
			outputQueries(queryFile, noOfSequences, numPartition, dataDir,
					outputPrefix);
		} catch (Exception e) {
			e.printStackTrace();
		}

		double timeInSeconds = ((double) (System.currentTimeMillis() - beforeTime)) / 1000;
		System.out.println("Total Time for BLAST Query Partition : " + timeInSeconds
				+ "Seconds");
		System.exit(0);
	}
}

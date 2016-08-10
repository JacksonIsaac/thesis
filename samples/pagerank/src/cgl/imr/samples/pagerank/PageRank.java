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
import java.util.ArrayList;
import java.util.List;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.KeyValuePair;
import cgl.imr.base.TwisterModel;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
//import cgl.imr.monitor.TwisterMonitor;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.types.BytesValue;
import cgl.imr.types.DoubleVectorData;
import cgl.imr.types.IntKey;

/**
 * Implements pagerank algorithm using Twister programming model. for pagerank
 * algorithm using MapReduce,please check the following link:
 * http://en.wikipedia.org/wiki/PageRank
 * 
 * pagerank algorithm for mapreduce <code>
 * 	begin
 *      Broadcast input data
 *      end
 * 
 *      begin
 *      [Perform in parallel] the map() operation
 *      for each data chunk of the network adjacency matrix
 *      	for each url
 *      	  	update the pagerank for every related url
 *      	endfor
 *      endfor
 *      end
 * 
 *      [Perform in parallel] the reduce() operation
 *      begin
 *      merge certain number of partial result sets
 *      end
 * 
 *      [Perform in sequentially] the combine() operation
 *      begin
 *      merge all the partial merged results together 
 *      end
 * <code>
 * 
 * @author Hui Li (lihui@indiana.edu)
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * 
 */

public class PageRank {
	/**
	 * Produces a list of key,value pairs for map tasks.
	 * 
	 * @param data
	 *            - We send data as bytes directly.
	 * @param numMaps
	 *            - Number of map tasks.
	 * @return - List of key,value pairs.
	 */
	private static List<KeyValuePair> getKeyValuesForMap(byte[] data,
			int numMaps) {
		List<KeyValuePair> keyValues = new ArrayList<KeyValuePair>();
		IntKey key = null;
		BytesValue value = null;
		for (int i = 0; i < numMaps; i++) {
			key = new IntKey(i);
			value = new BytesValue(data);
			keyValues.add(new KeyValuePair(key, value));
		}
		return keyValues;
	}
	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			String errorReport = "PageRank: the Correct arguments are \n"
					+ "java cgl.imr.samples.pagerank.PageRank "
					+ "[num urls][num map tasks][num reduce tasks][partition file][output file]";
			System.out.println(errorReport);
			System.exit(0);
		}
		int numUrls = Integer.parseInt(args[0]);
		int numMapTasks = Integer.parseInt(args[1]);
		int numReduceTasks = Integer.parseInt(args[2]);
		String partitionFile = args[3];
		String outputFile = args[4];
		PageRank client;
		try {
			client = new PageRank(numUrls, numMapTasks, numReduceTasks,
					partitionFile, outputFile);
			double beginTime = System.currentTimeMillis();
			client.driveMapReduce();
			double endTime = System.currentTimeMillis();
			System.out
					.println("------------------------------------------------------");
			System.out.println("Twister Pagerank job take " + (endTime - beginTime) / 1000
					+ " seconds.");
			System.out
					.println("------------------------------------------------------");
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}
	private int numMapTasks;
	private int numReduceTasks;
	private int numUrls; // num of urls
	private String outputFile; // output file store the final page rank results.

	/*
	 * Start the process of MapReduce algorithm
	 * 
	 * @parameter numUrls - number of urls in the page rank job
	 * 
	 * @parameter partitionFile - file store information how to distribute input
	 * files on multiple nodes
	 * 
	 * @parameter numMapTasks - number of map tasks, usually equal to the number
	 * of input files
	 * 
	 * @parameter outputFile - file store the final results of page rank values.
	 */

	private String partitionFile;

	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	public PageRank(int numUrls, int numMapTasks, int numReduceTasks,
			String partitionFile, String outputFile) {
		this.numUrls = numUrls;
		this.numMapTasks = numMapTasks;
		this.partitionFile = partitionFile;
		this.outputFile = outputFile;
		this.numReduceTasks = numReduceTasks;
	}

	private DoubleVectorData decompress(DoubleVectorData compressedData) {
		double[][] comData = compressedData.getData();
		int numData = compressedData.getNumData();
		int numUrls = (int) comData[0][0];
		double tanglingProb = comData[0][1];
		double[][] newPageRanksData = new double[numUrls][2];
		int index;
		for (int i = 1; i < numData; i++) {
			index = (int) comData[i][0];
			newPageRanksData[index][1] += comData[i][1];
		}

		for (int i = 0; i < numUrls; i++) {
			newPageRanksData[i][0] = i;
			newPageRanksData[i][1] += tanglingProb / (double)numUrls;
			newPageRanksData[i][1] = 0.15 * (1.0 / (double)numUrls) + 0.85
					* newPageRanksData[i][1];
		}
		DoubleVectorData resData = new DoubleVectorData(newPageRanksData,
				numUrls, 2);
		return resData;
	}

	/*
	 * decompressed the compressed pagerank data.
	 * 
	 * @parameter compressedData -variable that store the values of urls whose
	 * probability values changed in last processing
	 * 
	 * @return value -decompressed data store values of all the urls,include
	 * changed and unchanged
	 */

	public void driveMapReduce() throws Exception {
		long beforeTime = System.currentTimeMillis();

		// JobConfigurations
		JobConf jobConf = new JobConf("pagerank-map-reduce"
				+ uuidGen.generateTimeBasedUUID());

		jobConf.setMapperClass(PageRankMapTask.class);
		jobConf.setReducerClass(PageRankReduceTask.class);
		jobConf.setCombinerClass(PageRankCombiner.class);
		jobConf.setNumMapTasks(this.numMapTasks);
		jobConf.setNumReduceTasks(this.numReduceTasks);
		//jobConf.setFaultTolerance();

		TwisterDriver driver = new TwisterDriver(jobConf);
		driver.configureMaps(this.partitionFile);
		// divide the static input data for map tasks,
		// input data is the adjacency matrix for urls

		DoubleVectorData tmpDvd = new DoubleVectorData();
		DoubleVectorData tmpCompressedDvd;
		DoubleVectorData newDvd = new DoubleVectorData();
		DoubleVectorData newCompressedDvd = new DoubleVectorData();

		double[][] initPageRanks = new double[1][2];
		initPageRanks[0][0] = this.numUrls; // the num of all urls
		initPageRanks[0][1] = 1.0; // the sum of prob of all urls = 1.0
		tmpCompressedDvd = new DoubleVectorData(initPageRanks, 1, 2);

		/*
		 * [the data structure of the compressed pagerank matrix] the first item
		 * store the common tangling access probablity for all the urls, other
		 * items store the access probability of the updated urls. e.g.
		 * double[][] pagerank = new double[numChangedUrls][2]; pagerank[0][0]
		 * the num of all urls pagerank[0][1] the sum of access probablity of
		 * all tangling urls pagerank[i][0] the index of one url pagerank[i][1]
		 * the access probablity of one url note: i>0;
		 */

		double totalError = 0; // the error between current and previous rank values
		double tolerance = 1E-8; // the threshold value that determine converge condition
		int loopCount = 0;
		TwisterMonitor monitor = null;
		boolean complete = false;

		while (!complete) {
			// start the pagerank map reduce process
			monitor = driver.runMapReduceBCast(new BytesValue(tmpCompressedDvd.getBytes()));
			monitor.monitorTillCompletion();
			newCompressedDvd = ((PageRankCombiner) driver.getCurrentCombiner()).getResults(); // get the result of
			newDvd = decompress(newCompressedDvd); // decompress the compressed
			tmpDvd = decompress(tmpCompressedDvd);
			totalError = getError(tmpDvd, newDvd); // get the difference between
			System.out.println("[log] Error between current and previous rank values:"
					+ totalError);
			if (totalError < tolerance) {
				complete = true;
			}
			tmpCompressedDvd = newCompressedDvd;
			loopCount++;
		}
		System.out.println("[log] The error of rank values converged, total loop count:"
				+ loopCount);
		double timeInSeconds = ((double) (System.currentTimeMillis() - beforeTime)) / 1000;

		// store the final result of pagerank values into disk file.
		double[][] urlsData = tmpDvd.getData();
		BufferedWriter writer = new BufferedWriter(new FileWriter(
				this.outputFile));
		String strLine = this.numUrls + "\n";
		writer.write(strLine);
		for (int i = 0; i < numUrls; i++) {
			strLine = (int) (urlsData[i][0]) + " " + urlsData[i][1] + "\n";
			writer.write(strLine);
		}
		writer.flush();
		writer.close();
		driver.close();
	}

	private double getError(DoubleVectorData tmpDvd, DoubleVectorData newDvd) {
		double totalError = 0;
		int numData = tmpDvd.getNumData();
		double[][] tmpData = tmpDvd.getData();
		double[][] newData = newDvd.getData();

		for (int i = 0; i < numData; i++) {
			totalError += (tmpData[i][1] - newData[i][1])
					* (tmpData[i][1] - newData[i][1]);
		}
		return totalError;
	}
}

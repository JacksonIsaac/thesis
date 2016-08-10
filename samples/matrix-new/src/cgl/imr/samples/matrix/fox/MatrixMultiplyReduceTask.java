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

package cgl.imr.samples.matrix.fox;

import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.UnknownHostException;
import java.util.List;

import cgl.imr.base.Key;
import cgl.imr.base.ReduceOutputCollector;
import cgl.imr.base.ReduceTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.ReducerConf;
import cgl.imr.types.IntKey;

public class MatrixMultiplyReduceTask implements ReduceTask {

	int numMapTasks = 0;
	int finalWidth = 0;
	int bz = 0;
	int n = 0;
	int d = 0;
	int reduceNo = 0;

	int count = 0; // Has side effects. This can be fixed by getting the count
					// from map output.
	double[][] outputData;

	public void close() throws TwisterException {
		// TODO Auto-generated method stub
	}

	@Override
	public void configure(JobConf jobConf, ReducerConf reducerConf)
			throws TwisterException {
		numMapTasks = jobConf.getNumMapTasks();
		finalWidth = Integer.parseInt(jobConf.getProperty("final_width"));
		bz = Integer.parseInt(jobConf.getProperty("block_size"));
		n = Integer.parseInt(jobConf.getProperty("n"));
		d = Integer.parseInt(jobConf.getProperty("d"));
		outputData = new double[d][d];
		reduceNo = reducerConf.getReduceTaskNo();
//		try {
//			System.out.println(reduceNo+" assigned to "+InetAddress.getLocalHost().getHostName());
//		} catch (UnknownHostException e) {			
//			e.printStackTrace();
//		}
	}

	@Override
	public void reduce(ReduceOutputCollector collector, Key key,
			List<Value> values) throws TwisterException {

		count++;
		if (values.size() != 2) {
			System.out
					.println("Maps are not sending data correctly. Reduce should receive 2 blocks");
		}

		MatrixData A_Block;
		MatrixData B_Block;

		MatrixData tmpBlock = (MatrixData) values.get(0);
		if (tmpBlock.getMatrixNo() == 0) {
			A_Block = tmpBlock;
			B_Block = (MatrixData) values.get(1);
		} else {
			B_Block = tmpBlock;
			A_Block = (MatrixData) values.get(1);
		}
		//System.out.println("Reduce "+reduceNo+" received data");
		//double begin=System.currentTimeMillis();	
		MatrixUtils.matrixMultiply(A_Block.getData(),B_Block.getData(), d, d, d,outputData);
		//double end=System.currentTimeMillis();
		//System.out.println("R "+reduceNo+"  time="+(end-begin)/1000+" seconds");
			
		
//		for (int i = 0; i < d; i++) {
//			for (int j = 0; j < d; j++) {
//				outputData[i][j] += output[i][j];
//			}
//		}

		MatrixData outData;
		double[][] rowBlock = null;
		if (count != n) {
			rowBlock = new double[2][2];
			outData = new MatrixData(rowBlock, 2, 2);

		} else {
			//rowBlock = outputData;
			//outData = new MatrixData(rowBlock, d, d);	
			rowBlock = new double[2][2];
			outData = new MatrixData(rowBlock, 2, 2);
		}
		outData.setRow(reduceNo / n);
		outData.setCol(reduceNo % n);
		collector.collect(new IntKey(reduceNo), outData);
		
		
	}

}

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

package cgl.imr.samples.wordcount;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Generates a set of data files to match a given number of gigabytes.
 * 
 * @author Hui Li 
 * 
 */

public class WCFileSplit {
	
	public static void main(String[] args) throws IOException {

		if (args.length != 4) {
			System.out.println("Usage: [input file path][output dir][number of splits][partitioned file name pattern]");
			System.exit(0);
		}
		String inputFile = args[0];
		String outputDir = args[1];
		int numSplits = Integer.valueOf(args[2]);
		String filePrefix = args[3];
		
		String filePart = outputDir + "/" + filePrefix;
		String strLine;
		BufferedWriter writer;
		BufferedReader reader;
		reader = new BufferedReader(new FileReader(inputFile));
		int totalLine = 0;
		while((strLine=reader.readLine())!=null)
			totalLine++;
		reader.close();
		int[] counter = new int[numSplits];
		int block = totalLine/numSplits;
		int modular = totalLine%numSplits;
		for (int i=0;i<numSplits;i++){
			counter[i] = block;
			if (modular>0){
				counter[i]++;
				modular--;
			}
		}
		reader = new BufferedReader(new FileReader(inputFile));
		for (int i = 0; i < numSplits; i++) {
			File file = new File(filePart + i + ".txt");
			writer = new BufferedWriter(new FileWriter(file));
			for (int j=0;j<counter[i];j++){
				strLine=reader.readLine();
				assert(strLine!=null);
				writer.write(strLine+"\n");
			}
			writer.close();
		}
		reader.close();
	}
}
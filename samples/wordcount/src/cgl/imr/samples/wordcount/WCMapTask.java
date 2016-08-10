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

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.MapTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.data.file.FileData;
import cgl.imr.types.IntValue;
import cgl.imr.types.StringKey;



/**
 * Map task for the word count application.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * 
 */

public class WCMapTask implements MapTask {

	private FileData fileData;
	private Map<String, Integer> words;

	public WCMapTask() {
		words = new HashMap<String, Integer>();
	}

	public void close() throws TwisterException {
		// TODO Auto-generated method stub
	}

	public void configure(JobConf jobConf, MapperConf mapConf)
	throws TwisterException {
		fileData = (FileData) mapConf.getDataPartition();
	}
	
	/*
	* remove unnecessary letters such as . , ? !
	* parameter: inputStr
	*/
	private String[] splitStr(String inputStr){
		inputStr = inputStr.trim().toLowerCase();
		String[] filterChar = {".",",","?","!",";",":","\"","-"};
		for (String c:filterChar){ 
		inputStr.replace(c," ");
		}
		return inputStr.split(" ");
	}//trimChar()

	@Override
	public void map(MapOutputCollector collector, Key key, Value val)
	throws TwisterException {
		try {
			BufferedReader br = new BufferedReader(new FileReader(fileData
					.getFileName()), 65536);
			String inputLine = null;
			while ((inputLine = br.readLine())!=null) {
				for (String w : splitStr(inputLine)) {
					updateWordCount(w.trim());
				}
			}
			br.close();
			// Now add the collected words and their respective counts to the
			// output collector.
			Iterator<String> ite = words.keySet().iterator();
			String strKey = null;
			while (ite.hasNext()) {
				strKey = ite.next();
				collector.collect(new StringKey(strKey), new IntValue(words
						.get(strKey).intValue()));
			}
		} catch (Exception e) {
			throw new TwisterException(e);
		}
	}

	/**
	 * This method performs the task of a local aggregator. Instead of
	 * collecting single <word,1> values, this will collect <word,count> values.
	 * 
	 * @param w
	 *            - word
	 */
	public void updateWordCount(String w) {

		Integer count = words.get(w);
		if (count == null) {
			count = 0;
		}
		count++;
		words.put(w, count);
	}
}

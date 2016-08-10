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

package cgl.imr.base.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.RuntimeErrorException;

import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.ReducerSelector;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.message.ReduceInput;
import cgl.imr.types.IntValue;

/**
 * Collector for Map outputs.
 * 
 * @author Jaliya Ekanayake (jaliyae@gamil.com, jekanaya@cs.indiana.edu)
 * 
 */
public class MapOutputCollectorImpl implements MapOutputCollector {

	private Map<Integer,ReduceInput> reduceInputs;
	private Map<Integer,Integer> outMap;
	private ReducerSelector reduceSelector;
	private int iteration;
	private String bcastTopicBase;
	private boolean bcastSupported=false;
	private int sqrtReducers;
	private Map<Integer,ReduceInput> rowBCastValues;

	public MapOutputCollectorImpl(ReducerSelector reducerSelector, int iteration) {
		this.reduceSelector = reducerSelector;
		this.iteration=iteration;
		// Creates the bins before we start.
//		this.reduceInputs = new ArrayList<ReduceInput>(reducerSelector.getNumReducers());
//		for (int i = 0; i < reducerSelector.getNumReducers(); i++) {
//			ReduceInput reduceInput = new ReduceInput(iteration);
//
//			/**
//			 * Currently the reducer sink is determine by appending a number to
//			 * the reducer sink base.
//			 * 
//			 */
//
//			reduceInput.setSink(reducerSelector.getSinkBase() + i);
//			reduceInput.setJobId(reducerSelector.getJobId());
//			reduceInputs.add(reduceInput);
//		}
		this.reduceInputs = new HashMap<Integer,ReduceInput>();
		this.outMap=new HashMap<Integer,Integer>();
		this.rowBCastValues= new HashMap<Integer, ReduceInput>();
	}
	
	public MapOutputCollectorImpl(ReducerSelector reducerSelector, int iteration,String bcastTopicBase,int sqrtReducers) {
		this(reducerSelector, iteration);
		this.bcastTopicBase=bcastTopicBase;		
		this.bcastSupported=true;
		this.sqrtReducers=sqrtReducers;
	}

	public void collect(Key key, Value val) {
		int reduceNo=reduceSelector.getReducerNumber(key);	
		ReduceInput req = reduceInputs.get(reduceNo);
		if(req==null){
			req=new ReduceInput(iteration);
			//req.setReduceNo(reduceNo);
			req.setSink(reduceSelector.getSinkBase() + reduceNo);
			req.setJobId(reduceSelector.getJobId());
			reduceInputs.put(reduceNo,req);	
			incrementOutMap(reduceNo);
		}		
		req.addKeyValue(key, val);	
		
	}
	
	private synchronized void  incrementOutMap(int reduceNo) {
		
		if(!outMap.containsKey(reduceNo)){
			outMap.put(reduceNo,1);
		}else{
			Integer count=outMap.get(reduceNo);
			outMap.put(reduceNo,++count);
		}
		
	}

	public void collectBCastToRow(int rowNum,Key key,Value val){
		if(!bcastSupported){
			throw new RuntimeException("Please enable Broadcast to row option using JobConf at the TwisterDriver.");
		}
		
		int begin=rowNum*sqrtReducers;
		int end=begin+sqrtReducers;
		for(int i=begin;i<end;i++){
			incrementOutMap(i);	
		}
		
		ReduceInput req = rowBCastValues.get(rowNum);
		if(req==null){
			req=new ReduceInput(iteration);
			//req.setReduceNo(reduceNo);
			req.setSink(bcastTopicBase + rowNum);
			req.setJobId(reduceSelector.getJobId());
			rowBCastValues.put(rowNum,req);	
		}
		
		req.addKeyValue(key, val);
	}

	public List<ReduceInput> getReduceInputs() {
		List<ReduceInput> inputs= new ArrayList<ReduceInput>();
		inputs.addAll(this.reduceInputs.values());
		return inputs ;
	}
	
	public List<ReduceInput> getBCastReduceInputs() {
		List<ReduceInput> inputs= new ArrayList<ReduceInput>();
		inputs.addAll(this.rowBCastValues.values());
		return inputs ;
	}

	@Override
	public Map<Integer,Integer> getReduceInputMap() {		
		
		
//		List<ReduceInput> inputs=getReduceInputs();
//		
//		for(ReduceInput reduceInput:inputs){
//			//count=outMap.get(reduceInput.getReduceNo());			
//			outMap.put(reduceInput.getReduceNo(),1);	
//			//System.out.println("Sending "+reduceInput.getReduceNo());
//		}
		return outMap;
	}
}

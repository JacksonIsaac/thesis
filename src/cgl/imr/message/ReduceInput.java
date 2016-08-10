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

package cgl.imr.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cgl.imr.base.Key;
import cgl.imr.base.SerializationException;
import cgl.imr.base.Value;
import cgl.imr.util.CustomClassLoader;
import cgl.imr.worker.DaemonWorker;

/**
 * Message carrying the map outputs to the reducers. Holds a
 * <code> Map<Key,List<Value>> </code>.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class ReduceInput extends PubSubMessage {

	private String jobId;
	private String keyClass;
	private Map<Key, List<Value>> outputs;
	private String sink;
	//private int reduceNo;
	private String valueClass;
	private int iteration;
	private boolean hasData=true;

	public ReduceInput() {
		this.outputs = new HashMap<Key, List<Value>>();
	}

	public ReduceInput(int iteration) {
		this();
		this.iteration = iteration;
	}

	public ReduceInput(byte[] bytes) throws SerializationException {
		this();
		this.fromBytes(bytes);
	}
	
	public void setNoHasData()	{
		this.hasData=false;
	}
	
	public boolean isHasData()	{
			return hasData;		
	}

//	public int getReduceNo() {
//		return reduceNo;
//	}

//	public void setReduceNo(int reduceNo) {
//		this.reduceNo = reduceNo;
//	}

	public void addKeyValue(Key key, Value val) {
		List<Value> values = this.outputs.get(key);
		if (values == null) {
			values = new ArrayList<Value>();
			this.keyClass = key.getClass().getName();
			this.valueClass = val.getClass().getName();
			values.add(val);
			this.outputs.put(key, values);
		} else {
			values.add(val);
		}
	}

	@Override
	public void fromBytes(byte[] bytes) throws SerializationException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {
			// First byte is the message type
			byte msgType = din.readByte();
			if (msgType != REDUCE_INPUT) {
				throw new SerializationException(
						"Invalid set of bytes to deserialize "
								+ this.getClass().getName() + ".");
			}

			byte[] data = null;
			int len = 0;

			// Read the refId if any and set the boolean flag.
			readRefIdIfAny(din);
			this.iteration = din.readInt();
			//this.reduceNo = din.readInt();

			len = din.readInt();
			data = new byte[len];
			din.readFully(data);
			this.jobId = new String(data);

			// read the sink bytes
			len = din.readInt();
			data = new byte[len];
			din.readFully(data);
			this.sink = new String(data);
			
			//has real data or the indirect keys.
			this.hasData=din.readBoolean();

			// Number of key, list<value> pairs
			int numKeys = din.readInt();

			if (numKeys > 0) {
				len = din.readInt();
				data = new byte[len];
				din.readFully(data);
				this.keyClass = new String(data);

				len = din.readInt();
				data = new byte[len];
				din.readFully(data);
				this.valueClass = new String(data);

				try {

					CustomClassLoader classLoader = DaemonWorker
							.getClassLoader(jobId);
					if (classLoader == null) {
						throw new SerializationException(
								"Could not find a class loader for this job id.");
					}

					Key key = null;
					Class<?> kClass = Class
							.forName(keyClass, true, classLoader);
					Class<?> vClass = Class.forName(valueClass, true,
							classLoader);
					for (int i = 0; i < numKeys; i++) {
						len = din.readInt();
						data = new byte[len];
						din.readFully(data);

						key = (Key) kClass.newInstance();
						key.fromBytes(data);

						// Now see how many values are there under this key.
						Value val = null;
						int numValues = din.readInt();
						for (int j = 0; j < numValues; j++) {
							len = din.readInt();
							data = new byte[len];
							din.readFully(data);

							val = (Value) vClass.newInstance();
							val.fromBytes(data);

							// Add the key value pair.
							addKeyValue(key, val);
						}
					}
				} catch (Exception e) {
					throw new SerializationException("Could not load classes",
							e);
				}
			}
			din.close();
			baInputStream.close();

		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
	}

	@Override
	public byte[] getBytes() throws SerializationException {
		byte[] serializedBytes = null;

		try {
			Iterator<Key> keyIterator = outputs.keySet().iterator();
			List<Value> values = null;
			Key key = null;

			ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
			DataOutputStream dout = new DataOutputStream(baOutputStream);

			// First byte is the message type.
			dout.writeByte(REDUCE_INPUT);

			byte[] data = null;

			// Write the refID if any with the boolean flag.
			serializeRefId(dout);
			dout.writeInt(iteration);
			//dout.writeInt(reduceNo);

			// Now write the sink
			data = jobId.getBytes();
			dout.writeInt(data.length);
			dout.write(data);

			// Now write the sink
			data = sink.getBytes();
			dout.writeInt(data.length);
			dout.write(data);
			
			dout.writeBoolean(hasData);

			// Next, write the number of <key <List of values>> pairs in this
			// message
			// for the intended reducer.
			dout.writeInt(outputs.keySet().size());

			if (outputs.keySet().size() > 0) {
				data = keyClass.getBytes();
				dout.writeInt(data.length);
				dout.write(data);

				data = valueClass.getBytes();
				dout.writeInt(data.length);
				dout.write(data);
			}

			while (keyIterator.hasNext()) {
				key = keyIterator.next();

				data = key.getBytes();
				dout.writeInt(data.length);
				dout.write(data);

				values = outputs.get(key);
				dout.writeInt(values.size());

				for (Value val : values) {

					data = val.getBytes();
					dout.writeInt(data.length);
					dout.write(data);
				}
			}

			dout.flush();
			serializedBytes = baOutputStream.toByteArray();
			baOutputStream = null;
			dout = null;

		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
		return serializedBytes;
	}

	public String getJobId() {
		return jobId;
	}

	public int getIteration() {
		return iteration;
	}

	public Map<Key, List<Value>> getOutputs() {
		return outputs;
	}

	public String getSink() {
		return sink;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public void setSink(String sink) {
		this.sink = sink;
	}
}

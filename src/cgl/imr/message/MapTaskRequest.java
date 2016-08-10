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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import cgl.imr.base.Key;
import cgl.imr.base.SerializationException;
import cgl.imr.base.Value;
import cgl.imr.util.CustomClassLoader;
import cgl.imr.worker.DaemonWorker;

/**
 * Request for map tasks.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class MapTaskRequest extends PubSubMessage {

	private String jobId;
	private String keyClass;
	private Map<Key, Value> keyValues;
	private int mapTaskNo = 0;
	private String responseTopic;
	private String sinkBase;
	private String valClass;

	private int iteration;

	protected MapTaskRequest() {
		this.keyValues = new HashMap<Key, Value>();
	}

	public MapTaskRequest(byte[] request) throws SerializationException {
		this();
		this.fromBytes(request);
	}

	public MapTaskRequest(int mapTaskNo, int iteration) {
		this();
		this.mapTaskNo = mapTaskNo;
		this.iteration = iteration;
	}

	public void addKeyValue(Key key, Value val) {
		if (this.keyValues.size() == 0) {
			this.keyClass = key.getClass().getName();
			this.valClass = val.getClass().getName();
		}
		this.keyValues.put(key, val);
	}

	/* Create an object using the serialized bytes. */
	@Override
	public void fromBytes(byte[] messageBytes) throws SerializationException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(
				messageBytes);
		DataInputStream din = new DataInputStream(baInputStream);
		int len = 0;
		byte[] data = null;

		try {
			// First byte is the message type
			byte msgType = din.readByte();
			if (msgType != MAP_TASK_REQUEST) {
				throw new SerializationException(
						"Invalid set of bytes to deserialize "
								+ this.getClass().getName() + ".");
			}

			// Read the refId if any and set the boolean flag.
			readRefIdIfAny(din);

			len = din.readInt();
			data = new byte[len];
			din.readFully(data);
			this.jobId = new String(data);

			this.mapTaskNo = din.readInt();
			this.iteration = din.readInt();

			int numKeys = din.readInt();
			if (numKeys > 0) {
				len = din.readInt();
				data = new byte[len];
				din.readFully(data);
				keyClass = new String(data);

				len = din.readInt();
				data = new byte[len];
				din.readFully(data);
				valClass = new String(data);

				CustomClassLoader classLoader = DaemonWorker
						.getClassLoader(jobId);
				if (classLoader == null) {
					throw new SerializationException(
							"Could not find a class loader for this job id.");
				}

				Class<?> kClass = Class.forName(keyClass, true, classLoader);
				Class<?> vClass = Class.forName(valClass, true, classLoader);

				Key key = null;
				Value val = null;
				for (int i = 0; i < numKeys; i++) {
					len = din.readInt();
					data = new byte[len];
					din.readFully(data);

					key = (Key) kClass.newInstance();
					key.fromBytes(data);

					len = din.readInt();
					data = new byte[len];
					din.readFully(data);

					val = (Value) vClass.newInstance();
					val.fromBytes(data);

					// Add the key value pair.
					addKeyValue(key, val);
				}
			}

			len = din.readInt();
			data = new byte[len];
			din.readFully(data);
			sinkBase = new String(data);

			len = din.readInt();
			data = new byte[len];
			din.readFully(data);
			responseTopic = new String(data);

			din.close();
			baInputStream.close();

		} catch (Exception ioe) {
			throw new SerializationException(ioe);
		}
	}

	/* Serialize the object. */
	@Override
	public byte[] getBytes() throws SerializationException {

		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();

		DataOutputStream dout = new DataOutputStream(baOutputStream);
		byte[] marshalledBytes = null;
		byte[] data = null;

		Iterator<Key> keyIterator = keyValues.keySet().iterator();
		Value value = null;
		Key key = null;

		try {
			dout.writeByte(MAP_TASK_REQUEST);

			// Write the refID if any with the boolean flag.
			serializeRefId(dout);

			data = jobId.getBytes();
			dout.writeInt(data.length);
			dout.write(data);

			dout.writeInt(mapTaskNo);
			dout.writeInt(iteration);

			dout.writeInt(keyValues.keySet().size());
			if (keyValues.keySet().size() > 0) {
				data = keyClass.getBytes();
				dout.writeInt(data.length);
				dout.write(data);

				data = valClass.getBytes();
				dout.writeInt(data.length);
				dout.write(data);

				while (keyIterator.hasNext()) {
					key = keyIterator.next();

					data = key.getBytes();
					dout.writeInt(data.length);
					dout.write(data);

					value = keyValues.get(key);

					data = value.getBytes();
					dout.writeInt(data.length);
					dout.write(data);
				}
			}

			data = sinkBase.getBytes();
			dout.writeInt(data.length);
			dout.write(data);

			data = responseTopic.getBytes();
			dout.writeInt(data.length);
			dout.write(data);

			dout.flush();
			marshalledBytes = baOutputStream.toByteArray();
			baOutputStream = null;
			dout = null;
		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
		return marshalledBytes;
	}

	public String getJobId() {
		return jobId;
	}

	public Map<Key, Value> getKeyValues() {
		return keyValues;
	}

	public int getMapTaskNo() {
		return mapTaskNo;
	}

	public int getIteration() {
		return iteration;
	}

	public String getResponseTopic() {
		return responseTopic;
	}

	public String getSinkBase() {
		return sinkBase;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public void setKeyValues(Map<Key, Value> keyValues) {
		Iterator<Key> ite = keyValues.keySet().iterator();
		Key key = null;
		while (ite.hasNext()) {
			key = ite.next();
			this.addKeyValue(key, keyValues.get(key));
		}
	}

	public void setResponseTopic(String responseTopic) {
		this.responseTopic = responseTopic;
	}

	public void setSinkBase(String sinkBase) {
		this.sinkBase = sinkBase;
	}
}

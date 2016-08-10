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

package cgl.imr.base;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class KeyValueMap implements TwisterSerializable {

	private String keyClass;

	private Map<Key, Value> keyValues;
	private String valueClass;

	public KeyValueMap() {
		this.keyValues = new HashMap<Key, Value>();
	}

	public void addKeyValue(Key key, Value val) {
		if (this.keyValues.size() == 0) {
			this.keyClass = key.getClass().getName();
			this.valueClass = val.getClass().getName();
		}
		this.keyValues.put(key, val);
	}

	public void fromBytes(byte[] bytes) throws SerializationException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {
			byte[] data = null;
			int len = 0;

			// Number of <key, value> pairs
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

				Class<?> kClass = Class.forName(keyClass);
				Class<?> vClass = Class.forName(valueClass);

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
			din.close();
			baInputStream.close();

		} catch (Exception e) {
			throw new SerializationException("Could not load classes", e);
		}
	}

	public byte[] getBytes() throws SerializationException {

		byte[] serializedBytes = null;
		byte[] data = null;

		try {
			Iterator<Key> keyIterator = keyValues.keySet().iterator();
			Value value = null;
			Key key = null;

			ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
			DataOutputStream dout = new DataOutputStream(baOutputStream);

			dout.writeInt(keyValues.keySet().size());
			if (keyValues.keySet().size() > 0) {
				data = keyClass.getBytes();
				dout.writeInt(data.length);
				dout.write(data);

				data = valueClass.getBytes();
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
			dout.flush();
			serializedBytes = baOutputStream.toByteArray();
			baOutputStream = null;
			dout = null;

		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
		return serializedBytes;
	}

	public Map<Key, Value> getKeyValues() {
		return keyValues;
	}

	public void setKeyValues(Map<Key, Value> keyValues) {
		Iterator<Key> ite = keyValues.keySet().iterator();
		Key key = null;
		while (ite.hasNext()) {
			key = ite.next();
			this.addKeyValue(key, keyValues.get(key));
		}
	}
}

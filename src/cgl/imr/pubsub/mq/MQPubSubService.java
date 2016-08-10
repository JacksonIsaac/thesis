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

package cgl.imr.pubsub.mq;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import cgl.imr.base.PubSubException;
import cgl.imr.base.PubSubService;
import cgl.imr.base.Subscribable;
import cgl.imr.base.TwisterConstants.EntityType;
import cgl.imr.config.ConfigurationException;

/**
 * Register this client with ActiveMQ, Similar functionality as the classes for
 * NaradaBrokering connection.
 *
 * Every topic has a session to hold a related message consumer. So for one
 * connection, there is a mapping between topics and Sessions (or Consumers)
 * Once message for a specific topic is received, onMessage will invoke
 * Subscriber's onEvent function to handle it.
 *
 * Notice: Use one session for several Subscribers may cause consumer hang at
 * setMessageListener. As a result, every session only hold one consumer.
 *
 * @author Bingjing Zhang (zhangbj@cs.indiana.edu) 6/13/2010
 *
 */
public class MQPubSubService implements PubSubService, MessageListener {

	// for setting client ID of the connection of this daemon
	// currently not needed for normal consumer (not durable)
	// private int entityId;

	// the class which will handle the message event
	private Subscribable subscriber;

	// the mapping between topics and Sessions
	private Map<String, Session> topics;

	// ActiveMQ configurations
	private AMQConfigurations config;

	// this class holds one connection for sending and receiving message
	private Connection connection;

	/**
	 * Constructor, create connection according to amq.properties
	 *
	 * @param type
	 * @param daemonNo
	 * @throws PubSubException
	 */
	public MQPubSubService(EntityType type, int daemonNo)
			throws PubSubException {

		try {
			this.config = new AMQConfigurations();
		} catch (ConfigurationException e) {
			throw new PubSubException("ActiveMQ: configuration error.", e);
		}

		// set client ID, similar as the way in NBPubSubService
		// this.entityId = daemonNo + 100000;

		// initialize subscriber as null
		this.subscriber = null;

		// establish hashmap for mapping
		this.topics = new ConcurrentHashMap<String, Session>();

		// create connection factory
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
				config.getURI());
		// a way to avoid message missing in ActiveMQ
		// broker cluster. It uses
		// "?consumer.retroactive=true".
		// however, this will result in
		// unrelated messages pass to Driver and Daemon.
		// Once driver and daemon are shut down and then restart,
		// ActiveMQ will try to restore unrelated messages to clients.
		// So now not Driver but only Daemon use this.
		if (type.equals(EntityType.DAEMON)) {
			connectionFactory.setUseRetroactiveConsumer(true);
		}

		// create connection
		try {
			this.connection = connectionFactory.createConnection();
			// this.connection.setClientID(this.entityId + "");
			this.connection.start();
		} catch (JMSException e) {
			throw new PubSubException("ActiveMQ: creating connection error.", e);
		}
	}

	@Override
	/**
	 * close connection.
	 * All sessions will be destroyed automatically.
	 */
	public void close() throws PubSubException {
		if (this.connection != null) {
			try {
				this.connection.close();
				this.topics.clear();
			} catch (JMSException e) {
				throw new PubSubException("ActiveMQ: closing connection error.", e);
			}
		}
	}

	@Override
	/**
	 * Create a session and a producer inside to send byte message
	 */
	public void send(String topic, byte[] message) throws PubSubException {
		try {

			Session session = this.connection.createSession(false,
					Session.AUTO_ACKNOWLEDGE);

			MessageProducer producer = session.createProducer(session
					.createTopic(topic));
			// default delivery mode is persistent
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

			BytesMessage bytes = session.createBytesMessage();
			bytes.writeBytes(message);

			producer.send(bytes);
			session.close();

		} catch (JMSException e) {
			throw new PubSubException(e);
		}
	}

	@Override
	/**
	 * send string message
	 */
	public void send(String topic, String message) throws PubSubException {
		send(topic, message.getBytes());
	}

	@Override
	/**
	 * set the class who will use this MQPubSubService.
	 * It is called subscriber
	 * once the message comes, this class will handle it.
	 */
	public void setSubscriber(Subscribable callback) throws PubSubException {
		if (callback != null) {
			this.subscriber = callback;
		} else {
			throw new PubSubException("ActiveMQ:  Susbcriber cannot be NULL.");
		}
	}

	@Override
	/**
	 * create session, and hold a consumer (durable subscriber) inside
	 */
	public void subscribe(String topic) throws PubSubException {
		if (this.subscriber != null) {
			if (!topics.containsKey(topic)) {
				try {
					// create session
					Session session = this.connection.createSession(false,
							Session.AUTO_ACKNOWLEDGE);

					Topic tpc = session.createTopic(topic);

					// now durable subscriber is not used, replace by normal
					// consumer

					// MessageConsumer consumer =
					// session.createDurableSubscriber(
					// tpc, tpc.getTopicName());

					MessageConsumer consumer = session.createConsumer(tpc);

					// set message listener to current object of this class
					consumer.setMessageListener(this);

					// use hashmap to record the mapping between the topic and
					// the consumer session
					topics.put(topic, session);

				} catch (JMSException e) {
					throw new PubSubException(
							"ActiveMQ: creating consumer error.", e);
				}
			}
		} else {
			throw new PubSubException("ActiveMQ: no subscriber error.");
		}
	}

	@Override
	/**
	 * Unsubscribe a topic,
	 * close the session,
	 * remove the mapping record.
	 *
	 */
	public void unsubscribe(String topic) throws PubSubException {
		try {
			Session session = (Session) topics.get(topic);
			if (session != null) {
				session.close();
				topics.remove(topic);
			}
		} catch (JMSException e) {
			throw new PubSubException("ActiveMQ: unsubscribing topic error.", e);
		}
	}

	@Override
	/**
	 * Once message arrives, build a byte message,
	 * use subscriber class to handle this event
	 */
	public void onMessage(Message mqMessage) {
		if (this.subscriber != null) {
			byte[] message = null;
			try {
				BytesMessage bytes = (BytesMessage) mqMessage;
				message = new byte[(int) bytes.getBodyLength()];
				bytes.readBytes(message);

				this.subscriber.onEvent(message);

			} catch (Exception e) {
				// try to get all exception including the exception
				// caused by invoking subscriber's onEvent
				e.printStackTrace();
			}
		}
	}
}

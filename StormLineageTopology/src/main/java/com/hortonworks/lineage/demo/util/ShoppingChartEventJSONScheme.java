package com.hortonworks.lineage.demo.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.storm.kafka.StringScheme;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.hortonworks.lineage.demo.events.ShoppingCartEvent;

/*
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.kafka.KeyValueScheme;
*/

import org.apache.storm.kafka.KeyValueScheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class ShoppingChartEventJSONScheme implements KeyValueScheme {
		private static final long serialVersionUID = 1L;
		private static final Charset UTF8 = Charset.forName("UTF-8");

		public List<Object> deserializeKeyAndValue(ByteBuffer key, ByteBuffer value) {
			String eventKey = StringScheme.deserializeString(key);
			String eventJSONString = StringScheme.deserializeString(value);
	        ShoppingCartEvent incomingTransaction = null;
	        ObjectMapper mapper = new ObjectMapper();
	        System.out.println("***** Key: " + eventKey + ", Value: " + eventJSONString);
	        try {
	        	JsonNode rootNode = mapper.readTree(eventJSONString);
	        	System.out.println("***** rootNode: " + rootNode.toString());
	        	JsonNode valueNode = rootNode.path("value");
	        	System.out.println("***** valueNode: " + valueNode.toString());
	        	incomingTransaction = mapper.readValue(valueNode, ShoppingCartEvent.class);
			} catch (JsonParseException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
	        return new Values(eventKey, incomingTransaction);
	    }

		public List<Object> deserialize(ByteBuffer value) {
			String eventJSONString = StringScheme.deserializeString(value);
			ShoppingCartEvent incomingTransaction = null;
	        ObjectMapper mapper = new ObjectMapper();
	        System.out.println("***** Value: " + eventJSONString);
	        try {
	        	JsonNode rootNode = mapper.readTree(eventJSONString);
	        	System.out.println("***** rootNode: " + rootNode.toString());
	        	JsonNode valueNode = rootNode.path("value");
	        	System.out.println("***** valueNode: " + valueNode.toString());
	        	incomingTransaction = mapper.readValue(valueNode, ShoppingCartEvent.class);
			} catch (JsonParseException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
	        return new Values(null, incomingTransaction);
		}
		
	    public Fields getOutputFields() {
	        return new Fields("TransactionKey", "IncomingTransaction");
	    }
}
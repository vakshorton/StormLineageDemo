package com.hortonworks.lineage.demo.bolts;

import java.util.Map;

import com.hortonworks.lineage.demo.events.IncomingTransaction;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
*/

public class PrintTransaction extends BaseRichBolt {
	private OutputCollector collector;

	public void execute(Tuple tuple) {
		IncomingTransaction transaction = (IncomingTransaction) tuple.getValueByField("IncomingTransaction");
		System.out.println(transaction.toString());	
		collector.ack(tuple);
		collector.emit("HiveStream", new Values(transaction.getTransactionId(), transaction.getAccountNumber(), transaction.getAmount()));
		collector.emit("HBaseStream", new Values(transaction.getTransactionId(), transaction.getAccountNumber(), transaction.getAmount()));
		collector.emit("KafkaStream", new Values(transaction.getTransactionId(), transaction));
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;	
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("HiveStream", new Fields("transactionId","accountNumber","amount"));
		declarer.declareStream("HBaseStream", new Fields("transactionId","accountNumber","amount"));
		declarer.declareStream("KafkaStream", new Fields("key","message"));
	}

}

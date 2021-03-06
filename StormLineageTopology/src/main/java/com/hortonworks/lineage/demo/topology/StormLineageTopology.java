package com.hortonworks.lineage.demo.topology;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
/*
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy; */
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;

import com.hortonworks.lineage.demo.bolts.PrintTransaction;
import com.hortonworks.lineage.demo.util.Constants;
import com.hortonworks.lineage.demo.util.ShoppingChartEventJSONScheme;
import com.hortonworks.lineage.demo.util.TransactionEventJSONScheme;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.KeyValueSchemeAsMultiScheme;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/*
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.KeyValueSchemeAsMultiScheme;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;
*/

public class StormLineageTopology {
	public static void main(String[] args) {
	     TopologyBuilder builder = new TopologyBuilder();
	     Constants constants = new Constants();   
	  	 /* 
	     RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(",");
	  	  SyncPolicy syncPolicy = new CountSyncPolicy(1000);
	  	  FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);
	  	  FileNameFormat transactionLogFileNameFormat = new DefaultFileNameFormat().withPath(constants.getHivePath());
	  	  HdfsBolt LogTransactionHdfsBolt = new HdfsBolt()
	  		     .withFsUrl(constants.getNameNode())
	  		     .withFileNameFormat(transactionLogFileNameFormat)
	  		     .withRecordFormat(format)
	  		     .withRotationPolicy(rotationPolicy)
	  		     .withSyncPolicy(syncPolicy);*/
	  	  System.out.println("********************** Starting Topology.......");
	  	  System.out.println("********************** Zookeeper Host: " + constants.getZkHost());
	  	  System.out.println("********************** Zookeeper Port: " + constants.getZkPort());
	  	  System.out.println("********************** Kafka Broker Host: " + constants.getKafkaBrokerHost());
	  	  System.out.println("********************** Kafka Broker Port: " + constants.getKafkaBrokerPort());
	  	  System.out.println("********************** Zookeeper ConnString: " + constants.getZkConnString());
	  	  System.out.println("********************** Zookeeper Kafka Path: " + constants.getZkKafkaPath());
	  	  System.out.println("********************** Zookeeper HBase Path: " + constants.getZkHBasePath());
	  	  
	      Config conf = new Config(); 
	      BrokerHosts hosts = new ZkHosts(constants.getZkConnString(), constants.getZkKafkaPath());
	      
	      SpoutConfig incomingTransactionsKafkaSpoutConfig = new SpoutConfig(hosts, constants.getIncomingTransactionsTopicName(), constants.getZkKafkaPath(), UUID.randomUUID().toString());
	      incomingTransactionsKafkaSpoutConfig.scheme = new KeyValueSchemeAsMultiScheme(new TransactionEventJSONScheme());
	      incomingTransactionsKafkaSpoutConfig.ignoreZkOffsets = true;
	      incomingTransactionsKafkaSpoutConfig.useStartOffsetTimeIfOffsetOutOfRange = true;
	      incomingTransactionsKafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
	      KafkaSpout incomingTransactionsKafkaSpout = new KafkaSpout(incomingTransactionsKafkaSpoutConfig); 
	      
	      SpoutConfig ProcessedTransactionKafkaSpoutConfig = new SpoutConfig(hosts, constants.getProcessedTransactionTopicName(), constants.getZkKafkaPath(), UUID.randomUUID().toString());
	      ProcessedTransactionKafkaSpoutConfig.scheme = new KeyValueSchemeAsMultiScheme(new TransactionEventJSONScheme());
	      ProcessedTransactionKafkaSpoutConfig.ignoreZkOffsets = true;
	      ProcessedTransactionKafkaSpoutConfig.useStartOffsetTimeIfOffsetOutOfRange = true;
	      ProcessedTransactionKafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
	      KafkaSpout customerTransactionValidationKafkaSpout = new KafkaSpout(ProcessedTransactionKafkaSpoutConfig);
	      
	      SpoutConfig ShoppingCartEventsKafkaSpoutConfig = new SpoutConfig(hosts, constants.getIncomingTransactionsTopicName(), constants.getZkKafkaPath(), UUID.randomUUID().toString());
	      ShoppingCartEventsKafkaSpoutConfig.scheme = new KeyValueSchemeAsMultiScheme(new ShoppingChartEventJSONScheme());
	      ShoppingCartEventsKafkaSpoutConfig.ignoreZkOffsets = true;
	      ShoppingCartEventsKafkaSpoutConfig.useStartOffsetTimeIfOffsetOutOfRange = true;
	      ShoppingCartEventsKafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
	      KafkaSpout ShoppingCartEventsKafkaSpout = new KafkaSpout(ShoppingCartEventsKafkaSpoutConfig);
	      
	      Map<String, Object> hbConf = new HashMap<String, Object>();
	      hbConf.put("hbase.rootdir", constants.getNameNode() + "/apps/hbase/data/");
	      hbConf.put("hbase.zookeeper.quorum", constants.getZkHost());
		  hbConf.put("hbase.zookeeper.property.clientPort", constants.getZkPort());
	      hbConf.put("zookeeper.znode.parent", constants.getZkHBasePath());
	      conf.put("hbase.rootdir", constants.getNameNode() + "/apps/hbase/data/");
	      conf.put("hbase.conf", hbConf);
	      
	      Properties kafkaConf = new Properties();
	      kafkaConf.put("metadata.broker.list", constants.getKafkaBrokerHost() + ":" + constants.getKafkaBrokerPort());
	      kafkaConf.put("request.required.acks", "1");
	      kafkaConf.put("serializer.class", "kafka.serializer.DefaultEncoder");
	      conf.put("kafka.broker.properties", kafkaConf);
	      
	      SimpleHBaseMapper transactionMapper = new SimpleHBaseMapper()
	              .withRowKeyField("cart_id")
	              .withColumnFields(new Fields("cart_id",
	    					"billing_order_id",
	    					"opportunity_id",
	    					"transaction_id",
	    					"monthly_charge",
	    					"one_time_charge",
	    					"spcl_instructions",
	    					"house_debt_amount",
	    					"deposit_amount",
	    					"sik_eligible",
	    					"house_debt_acceptance",
	    					"previous_balance",
	    					"disable_sik_opts",
	    					"creation_date",
	    					"last_update",
	    					"timestamp", 
	    					"house_debt_acctnum",
	    					"past_due_amount",
	    					"term_ids",
	    					"xhs_credit_check",
	    					"collected_deposit_amt", 
	    					"default_offer",
	    					"upsell_selected", 
	    					"dvr_selected", 
	    					"additional_dvrs_count", 
	    					"max_deposit_amount", 
	    					"deposit_offer_id", 
	    					"credit_check_pass", 
	    					"has_installments_for_install", 
	    					"amnesty_amount", 
	    					"prepay_for_acctstanding", 
	    					"pre_pay_amount", 
	    					"credit_tier_max_deposit_limit", 
	    					"credit_bypass", 
	    					"tax_amount", 
	    					"hd_tech_charge", 
	    					"hsd_repackage"))
	              .withColumnFamily("0"); 
	      
	      /*
	      DelimitedRecordHiveMapper processedTransactionHiveMapper = new DelimitedRecordHiveMapper()
	    		  .withColumnFields(new Fields("transactionId","accountNumber","amount"));
	      
	      HiveOptions processedTransactionHiveOptions = new HiveOptions(constants.getHiveMetaStoreURI(),
	    				 							constants.getHiveDbName(),
	    				 							"ProcessedTransactions",
	    				 							processedTransactionHiveMapper);
	      
	      DelimitedRecordHiveMapper postProcessedTransactionHiveMapper = new DelimitedRecordHiveMapper()
	    		  .withColumnFields(new Fields("IncomingTransaction"));
	    		 
	      HiveOptions postProcessedTransactionHiveOptions = new HiveOptions(constants.getHiveMetaStoreURI(),
	    				 							constants.getHiveDbName(),
	    				 							"PostProcessedTransactions",
	    				 							postProcessedTransactionHiveMapper);
	      */
	      
	      DelimitedRecordHiveMapper ShoppingCartEventHiveMapper = new DelimitedRecordHiveMapper()
	    		  .withColumnFields(new Fields(	
	    				  	"cart_id",
	    					"billing_order_id",
	    					"opportunity_id",
	    					"transaction_id",
	    					"monthly_charge",
	    					"one_time_charge",
	    					"spcl_instructions",
	    					"house_debt_amount",
	    					"deposit_amount",
	    					"sik_eligible",
	    					"house_debt_acceptance",
	    					"previous_balance",
	    					"disable_sik_opts",
	    					"creation_date",
	    					"last_update",
	    					"timestamp", 
	    					"house_debt_acctnum",
	    					"past_due_amount", 
	    					"term_ids",
	    					"xhs_credit_check",
	    					"collected_deposit_amt", 
	    					"default_offer",
	    					"upsell_selected", 
	    					"dvr_selected", 
	    					"additional_dvrs_count", 
	    					"max_deposit_amount", 
	    					"deposit_offer_id", 
	    					"credit_check_pass", 
	    					"has_installments_for_install", 
	    					"amnesty_amount", 
	    					"prepay_for_acctstanding", 
	    					"pre_pay_amount", 
	    					"credit_tier_max_deposit_limit", 
	    					"credit_bypass", 
	    					"tax_amount", 
	    					"hd_tech_charge", 
	    					"hsd_repackage"));
	    
	      HiveOptions ShoppingCartEventTransactionHiveOptions = new HiveOptions(constants.getHiveMetaStoreURI(),
	    				 							constants.getHiveDbName(),
	    				 							"cart",
	    				 							ShoppingCartEventHiveMapper)
	    		  									.withTxnsPerBatch(2)
	    		  									.withBatchSize(100)
	    		  									.withIdleTimeout(10);
	      
	      /*
	      KafkaBolt processedTransactionForwardKafkaBolt = new KafkaBolt()
	              .withTopicSelector(new DefaultTopicSelector(constants.getProcessedTransactionTopicName()))
	              .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
	      */
	      
	      //builder.setSpout("IncomingTransactionsKafkaSpout", incomingTransactionsKafkaSpout);
	      builder.setSpout("ShoppingCartEventsKafkaSpout", ShoppingCartEventsKafkaSpout);
	      builder.setBolt("PrintTransaction", new PrintTransaction(), 1).shuffleGrouping("ShoppingCartEventsKafkaSpout");
	      builder.setBolt("ProcessedTransactionPersistToHBase", new HBaseBolt("cart", transactionMapper).withConfigKey("hbase.conf"), 1).shuffleGrouping("PrintTransaction", "HBaseStream");
	      builder.setBolt("ProcessedTransactionPersistToHive", new HiveBolt(ShoppingCartEventTransactionHiveOptions),1).shuffleGrouping("PrintTransaction", "HiveStream");
	      //builder.setBolt("ProcessedTransactionForwardToKafka", processedTransactionForwardKafkaBolt, 1).shuffleGrouping("PrintTransaction", "KafkaStream");
	      /*
	      builder.setSpout("ProcessedTransactionKafkaSpout", customerTransactionValidationKafkaSpout);
	      builder.setBolt("PrintProcessedTransaction", new PrintTransaction(), 1).shuffleGrouping("ProcessedTransactionKafkaSpout");
	      builder.setBolt("PostProcessedTransactionPersistToHive", new HiveBolt(postProcessedTransactionHiveOptions),1).shuffleGrouping("PrintTransaction", "HiveStream");	      
	      */
	      conf.setNumWorkers(1);
	      conf.setMaxSpoutPending(5000);
	      conf.setMaxTaskParallelism(1);
	      
	      //submitToLocal(builder, conf);
	      submitToCluster(builder, conf);
	 }
	 
	 public static void submitToLocal(TopologyBuilder builder, Config conf){
		 LocalCluster cluster = new LocalCluster();
		 cluster.submitTopology("StormLineageTopology", conf, builder.createTopology()); 
	 }
	 
	 public static void submitToCluster(TopologyBuilder builder, Config conf){
		 try {
				StormSubmitter.submitTopology("StormLineageTopology", conf, builder.createTopology());
		      } catch (AlreadyAliveException e) {
				e.printStackTrace();
		      } catch (InvalidTopologyException e) {
				e.printStackTrace();
		      } catch (AuthorizationException e) {
				e.printStackTrace();
		      }
	 }
}

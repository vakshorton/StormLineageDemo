package com.hortonworks.lineage.demo.bolts;

import java.util.Map;

import com.hortonworks.lineage.demo.events.ShoppingCartEvent;

/*
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
*/

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class PrintTransaction extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	public void execute(Tuple tuple) {
		//IncomingTransaction transaction = (IncomingTransaction) tuple.getValueByField("IncomingTransaction");
		ShoppingCartEvent transaction = (ShoppingCartEvent) tuple.getValueByField("IncomingTransaction");
		System.out.println(transaction.toString());	
		collector.ack(tuple);
		collector.emit("HiveStream", new Values(transaction.getCart_id(), 
				transaction.getBilling_order_id(),
				transaction.getOpportunity_id(),	
				transaction.getTransaction_id(),
				transaction.getMonthly_charge(),
				transaction.getOne_time_charge(),
				transaction.getSpcl_instructions(),
				transaction.getHouse_debt_amount(),
				transaction.getDeposit_amount(),
				transaction.getSik_eligible(),
				transaction.getHouse_debt_acceptance(),
				transaction.getPrevious_balance(),
				transaction.getDisable_sik_opts(),
				transaction.getCreation_date(),
				transaction.getLast_update(),
				transaction.getTimestamp(),
				transaction.getPast_due_amount(),	
				transaction.getTerm_ids(),
				transaction.getXhs_credit_check(),
				transaction.getCollected_deposit_amt(),
				transaction.getDefault_offer(),
				transaction.getUpsell_selected(),
				transaction.getDvr_selected(),
				transaction.getAdditional_dvrs_count(),
				transaction.getMax_deposit_amount(),
				transaction.getDeposit_offer_id(),
				transaction.getCredit_check_pass(),
				transaction.getHas_installments_for_install(),
				transaction.getAmnesty_amount(),
				transaction.getPrepay_for_acctstanding(),
				transaction.getPre_pay_amount(),
				transaction.getCredit_tier_max_deposit_limit(),
				transaction.getCredit_bypass(),
				transaction.getTax_amount(),
				transaction.getHd_tech_charge(),
				transaction.getHsd_repackage()));
		
		collector.emit("HBaseStream", new Values(transaction.getCart_id(), 
				transaction.getBilling_order_id(),
				transaction.getOpportunity_id(),	
				transaction.getTransaction_id(),
				transaction.getMonthly_charge(),
				transaction.getOne_time_charge(),
				transaction.getSpcl_instructions(),
				transaction.getHouse_debt_amount(),
				transaction.getDeposit_amount(),
				transaction.getSik_eligible(),
				transaction.getHouse_debt_acceptance(),
				transaction.getPrevious_balance(),
				transaction.getDisable_sik_opts(),
				transaction.getCreation_date(),
				transaction.getLast_update(),
				transaction.getTimestamp(),
				transaction.getPast_due_amount(),	
				transaction.getTerm_ids(),
				transaction.getXhs_credit_check(),
				transaction.getCollected_deposit_amt(),
				transaction.getDefault_offer(),
				transaction.getUpsell_selected(),
				transaction.getDvr_selected(),
				transaction.getAdditional_dvrs_count(),
				transaction.getMax_deposit_amount(),
				transaction.getDeposit_offer_id(),
				transaction.getCredit_check_pass(),
				transaction.getHas_installments_for_install(),
				transaction.getAmnesty_amount(),
				transaction.getPrepay_for_acctstanding(),
				transaction.getPre_pay_amount(),
				transaction.getCredit_tier_max_deposit_limit(),
				transaction.getCredit_bypass(),
				transaction.getTax_amount(),
				transaction.getHd_tech_charge(),
				transaction.getHsd_repackage()));
		//collector.emit("KafkaStream", new Values(transaction.getTransactionId(), transaction));
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;	
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declareStream("HiveStream", new Fields("transactionId","accountNumber","amount"));
		declarer.declareStream("HiveStream", new Fields(
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
				"past_due_amount",
				"house_debt_acctnum", 
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
		
		declarer.declareStream("HBaseStream", new Fields(
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
				"past_due_amount",
				"house_debt_acctnum", 
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
		//declarer.declareStream("KafkaStream", new Fields("key","message"));
	}
}
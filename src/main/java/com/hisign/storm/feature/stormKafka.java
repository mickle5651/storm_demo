package com.hisign.storm.feature;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * 需要先往hbase中存储数据
 * hbase 中以token为row_key,以 item1:score1,item2:score2,item3:score3 为row 进行存储。
 * storm 从 kafka　的topic中读取消息先交给 FeatrueExtractBolt 进行分词，然后再将分完词的 token
 * 发到 FetchRecResult 从hbase中检索并打印。
 */
public class stormKafka {
	public static void main(String[] args) throws Exception {

		String topic = "test";
		String zkRoot = "/kafka_storm";
		String spoutId = "kafkaSpout";

		BrokerHosts brokerHosts = new ZkHosts("master:2181");
		SpoutConfig kafkaConf = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
		kafkaConf.forceFromStart = true;
		kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());

		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", kafkaSpout, 2);
		builder.setBolt("feature_extract", 
				new FeatureExtractBolt(), 5).shuffleGrouping("spout");
		builder.setBolt("fetch_rec_result", 
				new FetchRecResult(), 5).shuffleGrouping("feature_extract");
//		builder.setBolt("fetch_rec_result", 
//				new FetchRecResult(), 5).fieldsGrouping("feature_extract", new Fields("fea"));

		Config config = new Config();
		config.setDebug(false);

		if (args != null && args.length > 0) {
			config.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], config, builder.createTopology());
		} else {
			config.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("kafka", config, builder.createTopology());

			// Thread.sleep(10000);

			// cluster.shutdown();
		}
	}
}

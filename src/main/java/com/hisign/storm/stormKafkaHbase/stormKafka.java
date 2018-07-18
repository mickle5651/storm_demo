package com.hisign.storm.stormKafkaHbase;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class stormKafka {
	public static void main(String[] args) throws Exception {

		String topic = "test";
		String zkRoot = "/kafka_storm";
		String spoutId = "kafkaSpout";

		BrokerHosts brokerHosts = new ZkHosts("master:2181");
		SpoutConfig kafkaConf = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
		kafkaConf.forceFromStart = true;
		kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());

		/** kafkaSpout 是storm-kafka包中现成的spout*/
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf);
		TopologyBuilder builder = new TopologyBuilder();
		/** spout id,IRichSpout的实现类，强制并行度*/
		builder.setSpout("spout", kafkaSpout, 2);
		builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("spout");

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

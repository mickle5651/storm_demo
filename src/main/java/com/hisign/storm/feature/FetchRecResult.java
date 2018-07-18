package com.hisign.storm.feature;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class FetchRecResult extends BaseRichBolt {

	private OutputCollector outputCollector;
	public static final String TableName = "badou_music_inverted_table";
	public static final String ColumnFamily = "data";
	public static Configuration conf = HBaseConfiguration.create();

	private static HTable table;

	public static void selectRowKey(String tablename, String rowKey) throws IOException {
		System.out.println("*****************");
		table = new HTable(conf, tablename);
		System.out.println("*****************");
		Get g = new Get(rowKey.getBytes());
		System.out.println("*****************");
		Result rs = table.get(g);
		System.out.println("*****************");

		if (rs.isEmpty()) {
			System.out.println("******** no row key *********");
		} else {
			System.out.println("==> " + new String(rs.getRow()));

			System.out.println("==> size:" + rs.size());
			
			
			for (Cell kv : rs.rawCells()) {
				System.out.println("--------------------" + new String(kv.getRow()) + "----------------------------");
				System.out.println("Column Family: " + new String(kv.getFamily()));
				System.out.println("Column       :" + new String(kv.getQualifier()));
				System.out.println("value        : " + new String(kv.getValue()));
			}
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;

		conf.set("hbase.master", "192.168.87.200:60000");
		conf.set("hbase.zookeeper.quorum", "192.168.87.200,192.168.87.201,192.168.87.202");
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void execute(Tuple input) {

		String content = input.getString(0);
		System.out.println("[1]---------------------" + content);
		for (String word : content.split(" ")) {

			try {
				String rowkey = word.replaceAll("/", "").trim();
				System.out.println("rowkey: " + rowkey);
				selectRowKey(TableName, rowkey);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("++++++++++++++");
			}
		}

		outputCollector.ack(input);
		System.out.println("[2]---------------------");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
	}
}
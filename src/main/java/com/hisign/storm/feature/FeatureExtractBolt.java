package com.hisign.storm.feature;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Map;

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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FeatureExtractBolt extends BaseRichBolt {

	private CloseableHttpClient httpClient;
	private OutputCollector outputCollector;
	HttpGet httpGet;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
		// httpClient = HttpClients.createDefault();
		PoolingHttpClientConnectionManager pccm = new PoolingHttpClientConnectionManager();
		pccm.setMaxTotal(300); // 连接池最大并发连接数
		pccm.setDefaultMaxPerRoute(50); // 单路由最大并发数
		httpClient = HttpClients.custom().setConnectionManager(pccm).setConnectionManagerShared(true).build();
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void execute(Tuple input) {
		System.out.println(input);
		String content = input.getString(0);

		System.out.println(content);

		try {
			
			String url = "http://192.168.87.100:8808/?content=" + content;
			System.out.println(url);
			httpGet = new HttpGet(url);
			CloseableHttpResponse response = httpClient.execute(httpGet);
			HttpEntity entity = response.getEntity();

			System.out.println(response.getStatusLine());
			if (entity != null) {
				// 打印响应内容长度
				System.out.println("Response content length: " + entity.getContentLength());
				// 打印响应内容
				String responseString = new String(EntityUtils.toString(entity));
				responseString = new String(responseString.getBytes("ISO-8859-1"), "utf-8");
				System.out.println(responseString);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				outputCollector.emit(new Values(responseString));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				httpClient.close();// 释放资源
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		outputCollector.ack(input);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declare(new Fields("fea"));	
	}
}
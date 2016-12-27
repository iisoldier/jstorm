package com.isoldier.topology;

import java.util.HashMap;
import java.util.Map;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isoldier.bolts.WordCounter;
import com.isoldier.bolts.WordNomalizer;
import com.isoldier.spouts.WordReader;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class TopologyMain {

	private static Logger log = LoggerFactory.getLogger(TopologyMain.class);
	public static void main(String[] args){

		String filename = "word.txt";
		String runMode = "local";

		Map<String, Object> conf  = new HashMap<String,Object>();
		conf.put("wordsFile", filename);
		conf.put(Config.TOPOLOGY_DEBUG, true);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader(),1);
		builder.setBolt("word-normalizer", new WordNomalizer(),1)
				.shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter(),1)
				.shuffleGrouping("word-normalizer");

		if(runMode.equals("local")){
			LocalCluster cluster = new LocalCluster();
			
			cluster.submitTopology("Getting-Started-Topologie", conf,
					builder.createTopology());
			log.info("本地模式启动... ...");
			try {
				Thread.sleep(3*1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			cluster.shutdown();
		} else {
			try {
				StormSubmitter.submitTopology("Getting-Started-Topologie", conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				
				e.printStackTrace();
			} catch (InvalidTopologyException e) { 
				
				e.printStackTrace();
			}
			log.info("集群模式启动... ...");
		}
		
	}
}
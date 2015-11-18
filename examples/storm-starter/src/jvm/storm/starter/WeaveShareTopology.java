/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.spout.RandomIntSpout;

import java.util.ArrayList;
import java.util.Map;

/**
 * This is a basic example of a Storm topology.
 */
public class WeaveShareTopology {

	public static class SumBolt extends BaseRichBolt {
		OutputCollector _collector;
		int range = 1000;
		int slide = 100;
		int fragNum = range / slide;
		int fragSum = 0;
		ArrayList<Integer> fragments = null;
		ArrayList<Integer> buffer = null;
		long startTime = -1;

		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
			buffer = new ArrayList<Integer>();
			fragments = new ArrayList<Integer>();
		}

		@Override
		public void execute(Tuple tuple) {
			if (startTime == -1) {
				startTime = System.currentTimeMillis();
			}
			if (System.currentTimeMillis() - startTime < slide) {
				buffer.add(tuple.getInteger(0));
			} else {
				int buffSum = 0;
				for (Integer i : buffer) {
					buffSum += i;
				}
				fragments.add(buffSum);
				if (fragments.size() > fragNum) {
					fragments.remove(0);
				}
				fragSum = 0;
				for (Integer i : fragments) {
					fragSum += i;
				}
				buffer = new ArrayList<Integer>();
				buffer.add(tuple.getInteger(0));
				startTime = System.currentTimeMillis();
			}
			_collector.emit(tuple, new Values(fragSum));
			_collector.ack(tuple);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("sum"));
		}

	}

	public static class NaiiveSumBolt extends BaseRichBolt {
		OutputCollector _collector;
		int range = 1000;
		int slide = 100;
		int bufNum = range / slide;
		int sum = 0;
		ArrayList<ArrayList<Integer>> buffers = null;
		ArrayList<Integer> buffer = null;
		long startTime = -1;

		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
			buffer = new ArrayList<Integer>();
			buffers = new ArrayList<ArrayList<Integer>>();
		}

		@Override
		public void execute(Tuple tuple) {
			if (startTime == -1) {
				startTime = System.currentTimeMillis();
			}
			if (System.currentTimeMillis() - startTime < slide) {
				buffer.add(tuple.getInteger(0));
			} else {
				buffers.add(buffer);
				if (buffers.size() > bufNum) {
					buffers.remove(0);
				}
				sum = 0;
				for (ArrayList<Integer> ai : buffers) {
					for (Integer i : ai) {
						sum += i;
					}
				}
				buffer = new ArrayList<Integer>();
				buffer.add(tuple.getInteger(0));
				startTime = System.currentTimeMillis();
			}
			_collector.emit(tuple, new Values(sum));
			_collector.ack(tuple);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("sum"));
		}

	}

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("num_spout", new RandomIntSpout(), 1);
		// builder.setBolt("sum_bolt", new SumBolt(), 1).shuffleGrouping("num_spout");
		builder.setBolt("sum_bolt", new NaiiveSumBolt(), 1).shuffleGrouping("num_spout");

		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}

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

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Map;

/**
 * This is a basic example of a Storm topology.
 */
public class WeaveShareTopology {

	public static class SumBoltMult extends BaseRichBolt {
		OutputCollector _collector;
		ArrayList<Query> acqs = null;
		ArrayList<FragDescr> fragDescriptions = null;
		AggregateThread aggrThread = null;

		public SumBoltMult(ArrayList<Query> acqs_, ArrayList<FragDescr> fragDescriptions_) {
			acqs = acqs_;
			fragDescriptions = fragDescriptions_;
		}

		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
			aggrThread = new AggregateThread(_collector, acqs, fragDescriptions);
			Thread aggrCalc = new Thread(aggrThread);
			aggrCalc.start();
		}

		@Override
		public void execute(Tuple tuple) {
			aggrThread.buffer.add(tuple.getInteger(0));
			_collector.ack(tuple);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("sums"));
		}
	}

	public static class SumBoltPaired extends BaseRichBolt {
		OutputCollector _collector;
		int range = 1000;
		int slide = 300;
		int fragNum = 0;
		int finalSum = 0;
		int currFragLengthIndex = 0;
		ArrayList<Integer> fragLengths = null;
		ArrayList<Integer> fragments = null;
		ArrayList<Integer> buffer = null;
		long startTime = -1;

		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
			buffer = new ArrayList<Integer>();
			fragments = new ArrayList<Integer>();
			fragLengths = new ArrayList<Integer>();
			int f1 = range % slide;
			if (f1 == 0) {
				fragNum = range / slide;
				fragLengths.add(slide);
			} else {
				fragNum = (range / slide) * 2 + 1;
				fragLengths.add(f1);
				fragLengths.add(slide - f1);
			}

		}

		@Override
		public void execute(Tuple tuple) {
			if (startTime == -1) {
				startTime = System.currentTimeMillis();
			}
			if (System.currentTimeMillis() - startTime < fragLengths.get(currFragLengthIndex)) {
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
				if (fragments.size() == fragNum && currFragLengthIndex == 0) {
					finalSum = 0;
					for (Integer i : fragments) {
						finalSum += i;
					}
				}
				buffer = new ArrayList<Integer>();
				buffer.add(tuple.getInteger(0));
				startTime = System.currentTimeMillis();
				if (currFragLengthIndex == fragLengths.size() - 1) {
					currFragLengthIndex = 0;
				} else {
					currFragLengthIndex++;
				}
			}
			_collector.emit(tuple, new Values(finalSum));
			_collector.ack(tuple);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("sum"));
		}
	}

	public static class SumBoltSingle extends BaseRichBolt {
		OutputCollector _collector;
		int range = 1000;
		int slide = 300;
		int fragNum = range / slide;
		int finalSum = 0;
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
				if (fragments.size() == fragNum) {
					finalSum = 0;
					for (Integer i : fragments) {
						finalSum += i;
					}
				}
				buffer = new ArrayList<Integer>();
				buffer.add(tuple.getInteger(0));
				startTime = System.currentTimeMillis();
			}
			_collector.emit(tuple, new Values(finalSum));
			_collector.ack(tuple);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("sum"));
		}
	}

	public static class NaiiveSumBoltSingle extends BaseRichBolt {
		OutputCollector _collector;
		int range = 1000;
		int slide = 100;
		int bufNum = range / slide;
		int finalSum = 0;
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
				if (buffers.size() == bufNum) {
					finalSum = 0;
					for (ArrayList<Integer> ai : buffers) {
						for (Integer i : ai) {
							finalSum += i;
						}
					}
				}
				buffer = new ArrayList<Integer>();
				buffer.add(tuple.getInteger(0));
				startTime = System.currentTimeMillis();
			}
			_collector.emit(tuple, new Values(finalSum));
			_collector.ack(tuple);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("sum"));
		}

	}

	public static void main(String[] args) throws Exception {
		ObjectInputStream OIS = new ObjectInputStream(new FileInputStream("C:/storm_stuff/execPlan"));
		ExecutionPlan exec = (ExecutionPlan) OIS.readObject();
		OIS.close();

		int multiplier = 100;
		for (ArrayList<Query> alq : exec.treeQueries) {
			for (Query q : alq) {
				q.slide *= multiplier;
				q.range *= multiplier;
			}
		}
		for (ArrayList<FragDescr> alfd : exec.treeExecutions) {
			for (FragDescr fd : alfd) {
				fd.fragLength *= multiplier;
				//				for (AcqToNumFrags atnf : fd.acqsToNumFrags) {
				//					atnf.acq.slide *= multiplier;
				//					atnf.acq.range *= multiplier;
				//				}
			}
		}

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("num_spout", new RandomIntSpout(), 1);
		//		int i = 1;
		//		builder.setBolt("sum_bolt_" + i, new SumBoltMult(exec.treeQueries.get(i), exec.treeExecutions.get(i)), 1).allGrouping("num_spout");
		for (int i = 0; i < exec.treeQueries.size(); i++) {
			builder.setBolt("sum_bolt_" + i, new SumBoltMult(exec.treeQueries.get(i), exec.treeExecutions.get(i)), 1).allGrouping("num_spout");
		}

		Config conf = new Config();
		////////////////////
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(100000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}

//public static class SumBoltMult extends BaseRichBolt {
//OutputCollector _collector;
//ArrayList<Query> acqs = null;
//int currFragLengthIndex = 0;
//ArrayList<FragDescr> fragDescriptions = null;
//ArrayList<Integer> fragments = new ArrayList<Integer>();
//List buffer = Collections.synchronizedList(new ArrayList<Integer>());
//long startTime = -1;
//int largestWindow = -1;
//HashMap<Integer, Integer> sums = new HashMap<Integer, Integer>();
//
//public SumBoltMult(ArrayList<Query> acqs_, ArrayList<FragDescr> fragDescriptions_) {
//	acqs = acqs_;
//	fragDescriptions = fragDescriptions_;
//	for (Query q : acqs) {
//		sums.put(q.id, 0);
//		if (q.range > largestWindow) {
//			largestWindow = (int) q.range;
//		}
//	}
//}
//
//@Override
//public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
//	_collector = collector;
//	Thread aggrCalc = new Thread(new AggregateThread());
//	aggrCalc.start();
//}
//
//@Override
//public void execute(Tuple tuple) {
//	if (startTime == -1) {
//		startTime = System.currentTimeMillis();
//	}
//	long now = System.currentTimeMillis();
//	FragDescr fd = fragDescriptions.get(currFragLengthIndex);
//	int fragLength = fd.fragLength;
//	if (now - startTime < fragLength) {
//		buffer.add(tuple.getInteger(0));
//	} else {
//		int buffSum = 0;
//		for (Object i : buffer) {
//			buffSum += (Integer) i;
//		}
//		fragments.add(buffSum);
//		if (fragments.size() > largestWindow) {
//			fragments.remove(0);
//		}
//		for (AcqToNumFrags atnf : fd.acqsToNumFrags) {
//			if (fragments.size() >= atnf.numFrags) {
//				int sum = 0;
//				for (int i = fragments.size() - atnf.numFrags; i < fragments.size(); i++) {
//					sum += fragments.get(i);
//				}
//				sums.put(atnf.acq.id, sum);
//			}
//		}
//		while (true) {
//			if (currFragLengthIndex == fragDescriptions.size() - 1) {
//				currFragLengthIndex = 0;
//			} else {
//				currFragLengthIndex++;
//			}
//			fragLength += fragDescriptions.get(currFragLengthIndex).fragLength;
//			if (now - startTime >= fragLength) {
//				fragments.add(0);
//			} else {
//				break;
//			}
//		}
//		buffer = new ArrayList<Integer>();
//		buffer.add(tuple.getInteger(0));
//		startTime = System.currentTimeMillis();
//	}
//	String out = new String("\n");
//	for (Query a : acqs) {
//		out += a.id + ": " + sums.get(a.id) + "\n";
//	}
//	_collector.emit(tuple, new Values(out));
//	_collector.ack(tuple);
//}
//
//@Override
//public void declareOutputFields(OutputFieldsDeclarer declarer) {
//	declarer.declare(new Fields("sums"));
//}
//}

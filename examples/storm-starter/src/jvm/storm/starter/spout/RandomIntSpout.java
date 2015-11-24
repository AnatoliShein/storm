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
package storm.starter.spout;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.starter.InputRateThread;

public class RandomIntSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;
	Random _rand;
	long startTime;
	long count;
	BufferedWriter writer;
	InputRateThread inputThread;
	ThreadMXBean tmxb;

	public RandomIntSpout(double inputRate_) {
		inputThread = new InputRateThread(inputRate_);
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
		startTime = System.currentTimeMillis();
		count = 0;
		try {
			writer = new BufferedWriter(new FileWriter(new File("IR")));
		} catch (Exception e) {
			e.printStackTrace();
		}
		Thread inputCalc = new Thread(inputThread);
		inputCalc.start();
		tmxb = ManagementFactory.getThreadMXBean();
	}

	public static void busySleep(long nanos) {
		long elapsed;
		final long startTime = System.nanoTime();
		do {
			elapsed = System.nanoTime() - startTime;
		} while (elapsed < nanos);
	}

	@Override
	public void nextTuple() {
		//		Utils.sleep(inputThread.toSleep);
		busySleep(inputThread.toSleep);
		int num = _rand.nextInt(100);
		_collector.emit(new Values(num));
		inputThread.increment();
		//		System.out.println("\nCPU_time: " + tmxb.getThreadCpuTime(Thread.currentThread().getId()));
		//		DecimalFormat df = new DecimalFormat("#.000");
		//		try {
		//			writer.write("\nInputRate = " + df.format(((double) count) / (System.currentTimeMillis() - startTime) * 1000) + " per sec");
		//		} catch (IOException e) {
		//			e.printStackTrace();
		//		}
		//		System.out.println("\nInputRate = " + df.format(((double) count) / (System.currentTimeMillis() - startTime) * 1000) + " per sec");
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
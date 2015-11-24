package storm.starter;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class AggregateThread implements Runnable {
	OutputCollector _collector = null;
	ArrayList<Query> acqs = null;
	ArrayList<FragDescr> fragDescriptions = null;
	List<Integer> buffer = null;
	List<Integer> buffTemp = null;
	HashMap<Integer, Integer> sums = new HashMap<Integer, Integer>();
	ArrayList<Integer> fragments = new ArrayList<Integer>();
	int largestWindow = -1;
	int currFragLengthIndex = 0;
	ThreadMXBean tmxb;

	public AggregateThread(OutputCollector _collector_, ArrayList<Query> acqs_, ArrayList<FragDescr> fragDescriptions_) {
		_collector = _collector_;
		buffer = new ArrayList<Integer>();
		acqs = acqs_;
		fragDescriptions = fragDescriptions_;
		for (Query q : acqs) {
			sums.put(q.id, 0);
			if (q.range > largestWindow) {
				largestWindow = (int) q.range;
			}
		}
		tmxb = ManagementFactory.getThreadMXBean();
		if (!tmxb.isThreadCpuTimeEnabled()) {
			System.err.println("CPU time unavailable!!!");
		}
	}

	public synchronized void reset() {
		buffTemp = buffer;
		buffer = new ArrayList<Integer>();
	}

	public synchronized void add(int i) {
		buffer.add(i);
	}

	@Override
	public void run() {
		long startTime = System.currentTimeMillis();
		long totalTime = 0;
		while (true) {
			FragDescr fd = fragDescriptions.get(currFragLengthIndex);
			int fragLength = fd.fragLength;
			totalTime += fragLength;
			Utils.sleep(totalTime - (System.currentTimeMillis() - startTime));
			reset();
			int buffSum = 0;
			for (Integer i : buffTemp) {
				buffSum += i;
			}
			fragments.add(buffSum);
			if (fragments.size() > largestWindow) {
				fragments.remove(0);
			}
			for (AcqToNumFrags atnf : fd.acqsToNumFrags) {
				if (fragments.size() >= atnf.numFrags) {
					int sum = 0;
					for (int i = fragments.size() - atnf.numFrags; i < fragments.size(); i++) {
						sum += fragments.get(i);
					}
					sums.put(atnf.acq.id, sum);
				}
			}
			String out = new String("\n@ " + (System.currentTimeMillis() - startTime) + "\n");
			for (Query a : acqs) {
				out += a.id + ": " + sums.get(a.id) + "\n";
			}
			_collector.emit(new Values(out));
			if (currFragLengthIndex == fragDescriptions.size() - 1) {
				currFragLengthIndex = 0;
			} else {
				currFragLengthIndex++;
			}
			//System.out.println("\nCPU_time: " + tmxb.getThreadCpuTime(Thread.currentThread().getId()));
		}
	}

}

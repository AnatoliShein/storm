package storm.starter;

import java.util.ArrayList;
import java.util.Collections;
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
	long startTime = -1;

	public AggregateThread(OutputCollector _collector_, ArrayList<Query> acqs_, ArrayList<FragDescr> fragDescriptions_) {
		_collector = _collector_;
		buffer = Collections.synchronizedList(new ArrayList<Integer>());
		acqs = acqs_;
		fragDescriptions = fragDescriptions_;
		for (Query q : acqs) {
			sums.put(q.id, 0);
			if (q.range > largestWindow) {
				largestWindow = (int) q.range;
			}
		}
	}

	@Override
	public void run() {
		while (true) {
			FragDescr fd = fragDescriptions.get(currFragLengthIndex);
			int fragLength = fd.fragLength;
			Utils.sleep(fragLength);
			synchronized (buffer) {
				buffTemp = buffer;
				buffer = Collections.synchronizedList(new ArrayList<Integer>());
			}
			int buffSum = 0;
			for (Object i : buffTemp) {
				buffSum += (Integer) i;
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
			String out = new String("\n");
			for (Query a : acqs) {
				out += a.id + ": " + sums.get(a.id) + "\n";
			}
			_collector.emit(new Values(out));
			if (currFragLengthIndex == fragDescriptions.size() - 1) {
				currFragLengthIndex = 0;
			} else {
				currFragLengthIndex++;
			}
		}
	}

}

package storm.starter;

import java.io.Serializable;

import backtype.storm.task.OutputCollector;
import backtype.storm.utils.Utils;

public class InputRateThread implements Runnable, Serializable {
	private static final long serialVersionUID = 1L;
	OutputCollector _collector = null;
	long count;
	long countTemp;
	double inputRate;
	//int refinementRate = 1;  //sec
	public long toSleep = 1; //nano sec

	public InputRateThread(double inputRate_) {
		count = 0;
		countTemp = 0;
		inputRate = inputRate_; //tuples per second
	}

	public synchronized void reset() {
		countTemp = count;
		count = 0;
	}

	public synchronized void increment() {
		count++;
	}

	@Override
	public void run() {
		long startTime = System.currentTimeMillis();
		long totalTime = 0;
		while (true) {
			totalTime += 1000;
			Utils.sleep(totalTime - (System.currentTimeMillis() - startTime));
			reset();
			System.out.println("countTemp: " + countTemp);
			//			System.out.println("toSleep: " + toSleep);
			double ratio = 0;
			if (countTemp == 0) {
				toSleep = 1;
			} else if (countTemp != inputRate) {
				ratio = inputRate / countTemp;
				toSleep = Math.round(toSleep / ratio);
			}
			if (toSleep == 0) {
				if (ratio > 0) {
					toSleep = 0;
				} else {
					toSleep = 1;
				}
			}
			//			System.out.println("Adjusted toSleep: " + toSleep);
		}
	}

}

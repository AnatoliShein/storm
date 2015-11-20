package storm.starter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @file Query.java
 * @author Anatoli Shein University of Pittsburgh
 * 
 */

public class Query implements Serializable {
	private static final long serialVersionUID = 1L;
	public int id;
	public long range;
	public long slide;
	public long frag1 = 0;
	public long frag2 = 0;

	public Query(Query q) {
		this.id = q.id;
		this.range = q.range;
		this.slide = q.slide;
		this.frag1 = q.frag1;
		this.frag2 = q.frag2;
	}

	public static ArrayList<Query> copy(ArrayList<Query> queries) {
		ArrayList<Query> copies = new ArrayList<Query>();
		for (Query q : queries) {
			copies.add(new Query(q));
		}
		return copies;
	}

	public Query() {
	}

	public Query(long range_, long slide_) {
		range = range_;
		slide = slide_;
		if (range % slide != 0) {
			frag1 = range % slide;
			frag2 = slide - frag1;
		}
	}

	public String toString() {
		return slide + "(" + frag1 + ")/" + range;
	}
}

class SlideComparator implements Comparator<Query> {

	@Override
	public int compare(Query q1, Query q2) {
		return (int) (q1.slide - q2.slide);
	}
}

class RangeComparator implements Comparator<Query> {

	@Override
	public int compare(Query q1, Query q2) {
		return (int) (q1.range - q2.range);
	}
}

class QueryComparator implements Comparator<Query> {

	@Override
	public int compare(Query q1, Query q2) {
		if (q1.slide == q2.slide) {
			return (int) (q1.frag1 - q2.frag1);
		} else {
			return (int) (q1.slide - q2.slide);
		}
	}
}

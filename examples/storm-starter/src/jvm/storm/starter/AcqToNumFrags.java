package storm.starter;

import java.io.Serializable;

public class AcqToNumFrags implements Serializable {

	private static final long serialVersionUID = 1L;
	Query acq;
	int numFrags;

	public AcqToNumFrags(Query acq, int numFrags) {
		this.acq = acq;
		this.numFrags = numFrags;
	}
}

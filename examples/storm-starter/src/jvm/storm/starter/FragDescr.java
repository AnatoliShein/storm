package storm.starter;

import java.io.Serializable;
import java.util.ArrayList;

public class FragDescr implements Serializable {

	private static final long serialVersionUID = 1L;
	int fragLength = 0;
	ArrayList<AcqToNumFrags> acqsToNumFrags;

	public FragDescr(int fragLength, ArrayList<AcqToNumFrags> acqIds) {
		this.fragLength = fragLength;
		this.acqsToNumFrags = acqIds;
	}

	public FragDescr() {
		acqsToNumFrags = new ArrayList<AcqToNumFrags>();
	}

}

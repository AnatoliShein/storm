package storm.starter;

import java.util.ArrayList;

public class FragDescr {
	int fragLength = 0;
	ArrayList<AcqToNumFrags> acqsToNumFrags;
	
	public FragDescr(int fragLength, ArrayList<AcqToNumFrags> acqIds) {
		this.fragLength = fragLength;
		this.acqsToNumFrags = acqIds;
	}

}

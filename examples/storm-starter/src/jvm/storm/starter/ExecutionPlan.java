package storm.starter;

import java.io.Serializable;
import java.util.ArrayList;

public class ExecutionPlan implements Serializable {

	private static final long serialVersionUID = 1L;
	ArrayList<ArrayList<Query>> treeQueries;
	ArrayList<ArrayList<FragDescr>> treeExecutions;

	public ExecutionPlan() {
		treeQueries = new ArrayList<ArrayList<Query>>();
		treeExecutions = new ArrayList<ArrayList<FragDescr>>();
	}
}

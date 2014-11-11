package org.apache.hadoop.hdfs.server.namenode;

import java.util.Comparator;

public class MarginalsComparatorDWRR implements Comparator<DatanodeInfoDWRR> {

	@Override
	public int compare(DatanodeInfoDWRR arg0, DatanodeInfoDWRR arg1) {
		// we put a minus because it's reversed (descending)
		return -arg0.getMarginalValue().compareTo(arg1.getMarginalValue());
	}

}

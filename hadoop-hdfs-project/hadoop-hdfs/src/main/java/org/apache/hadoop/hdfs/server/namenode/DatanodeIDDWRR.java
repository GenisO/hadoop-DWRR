package org.apache.hadoop.hdfs.server.namenode;

public class DatanodeIDDWRR implements Comparable<DatanodeIDDWRR> {

	private static int globalID = 0;
	
	public int id;
	
	public DatanodeIDDWRR() {
		this.id = globalID;
		globalID++;
	}
	
	public String toString() {
		return String.valueOf(id);
	}
	
	@Override
	public int compareTo(DatanodeIDDWRR arg0) {
		if (this.id < arg0.id) return -1;
		else if (this.id == arg0.id) return 0;
		else return 1;
	}

}

package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeID;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class DatanodeInfoDWRR implements Comparable<DatanodeInfoDWRR>{
  public static final Log LOG = LogFactory.getLog(DatanodeInfoDWRR.class);
	private DatanodeID nodeID;
	private BigDecimal capacity; // I/O max throughput in Mb/s
	private Map<ClassInfoDWRR, BigDecimal> weightByClass;
	private BigDecimal totalWeight;
  private static final float DEFAULT_CAPACITY = 100F;

  public DatanodeInfoDWRR(DatanodeID nodeID) {
    this(nodeID, DEFAULT_CAPACITY);
  }

    public DatanodeInfoDWRR(DatanodeID nodeID, float capacity) {
		this.nodeID = nodeID;
		this.capacity = new BigDecimal(capacity);
		this.weightByClass = new HashMap<ClassInfoDWRR, BigDecimal>();
		this.totalWeight = BigDecimal.ZERO;
	}
	
	public DatanodeID getDatanodeID() {
		return this.nodeID;
	}
	
	public BigDecimal getCapacity() {
		return this.capacity;
	}
	
	public BigDecimal getClassWeight(ClassInfoDWRR classInfo) {
    LOG.info("CAMAMILLA DatanodeInfoDWRR.getClassWeight nodeId="+nodeID.getDatanodeUuid()+" necessitem classid="+classInfo.getClassID());        // TODO TODO log
		if (!this.weightByClass.containsKey(classInfo))
					return BigDecimal.ZERO;		
		return this.weightByClass.get(classInfo);
	}
	
	public void updateClassWeight(ClassInfoDWRR classInfo, BigDecimal newWeight) {
    LOG.info("CAMAMILLA DatanodeInfoDWRR.updateClassWeight classid="+classInfo.getClassID()+" newWeight="+newWeight.floatValue());        // TODO TODO log
		BigDecimal oldWeight = this.weightByClass.put(classInfo, newWeight);
		if (oldWeight == null) oldWeight = BigDecimal.ZERO;
		if (newWeight.compareTo(BigDecimal.ZERO) == 0) this.weightByClass.remove(classInfo);
		this.totalWeight = this.totalWeight.subtract(oldWeight).add(newWeight);
    LOG.info("CAMAMILLA DatanodeInfoDWRR.updateClassWeight totalWeight="+totalWeight+" weightByClass="+weightByClass);        // TODO TODO log
	}

	public BigDecimal getClassShare(ClassInfoDWRR classInfo) {
		if (!this.weightByClass.containsKey(classInfo))
			return BigDecimal.ZERO;
		else if (this.weightByClass.size() == 1)
			return BigDecimal.ONE;
		return this.getClassWeight(classInfo).divide(this.getTotalWeight(), FairIOControllerDWRR.CONTEXT);
	}
	
	public BigDecimal getTotalWeight() {
		if (this.weightByClass.size() <= 1) {
			return FairIOControllerDWRR.MIN_TOTAL_WEIGHT.multiply(this.capacity);
		}
		return this.totalWeight;
	}
	
	public BigDecimal getMarginalValue() {
		return this.capacity.divide(this.getTotalWeight(), FairIOControllerDWRR.CONTEXT);
	}
	
	public String toString() {
		return String.format("[nid: %s, cap: %s, marginal: %s]", nodeID, FairIOControllerDWRR.decimalFormat.format(capacity), FairIOControllerDWRR.decimalFormat.format(this.getMarginalValue()));
	}
	
	@Override
	public int compareTo(DatanodeInfoDWRR o) {
		return this.nodeID.getDatanodeUuid().compareTo(o.getDatanodeID().getDatanodeUuid());
	}

  @Override
  public boolean equals(Object o) {
    if (this.nodeID.getDatanodeUuid().equals(((DatanodeInfoDWRR) o).getDatanodeID().getDatanodeUuid())) return true;
    else return false;
  }

  @Override
  public int hashCode() {
    return nodeID.getDatanodeUuid().hashCode();
  }

}

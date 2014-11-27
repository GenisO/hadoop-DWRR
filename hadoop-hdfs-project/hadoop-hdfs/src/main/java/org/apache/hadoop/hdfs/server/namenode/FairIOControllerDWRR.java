package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.util.Daemon;

import java.math.BigDecimal;
import java.math.MathContext;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FairIOControllerDWRR {
  public static final Log LOG = LogFactory.getLog(FairIOControllerDWRR.class);

  public static final int PRECISSION = 64;
  public static MathContext CONTEXT = new MathContext(PRECISSION);
  public static final BigDecimal MIN_TOTAL_WEIGHT = new BigDecimal(0.00000000000000000000000000000000000000000000001, CONTEXT);
  public static DecimalFormat decimalFormat = new DecimalFormat("0.0000");

  // Private constants taken from configuration file
  private static final BigDecimal MIN_UTILITY_GAP = new BigDecimal(0.0001, CONTEXT);

  // Private constants for ease code reading
  private static final BigDecimal MIN_SHARE = new BigDecimal(0.0001, CONTEXT);
  private static final BigDecimal ONE_MINUS_MIN_SHARE = BigDecimal.ONE.subtract(MIN_SHARE, CONTEXT);
  private static final BigDecimal MIN_COEFF = MIN_SHARE.divide(ONE_MINUS_MIN_SHARE, CONTEXT);
  private static final BigDecimal TWO = new BigDecimal(2, CONTEXT);
  public static final float DEFAULT_WEIGHT = 100;

  private Map<ClassInfoDWRR, Set<DatanodeInfoDWRR>> classToDatanodes;
  private HashMap<Long, ClassInfoDWRR> classInfoMap;
  private Map<DatanodeID, DatanodeInfoDWRR> nodeIDtoInfo;
  private MarginalsComparatorDWRR datanodeInfoComparator;
  private HashMap<String, DatanodeID> nodeUuidtoNodeID;
  private Daemon threadedFairDWRR = new Daemon(new ThreadGroup("FairIOControllerDWRR Thread"),
    new Runnable() {
      public final Log LOG = LogFactory.getLog(Daemon.class);

      @Override
      public void run() {
        while (true) {
          try {
            Thread.sleep(20*1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

          computeShares();
        }
      }
    });

  public FairIOControllerDWRR() {
    this.classToDatanodes = new HashMap<ClassInfoDWRR, Set<DatanodeInfoDWRR>>();
    this.nodeIDtoInfo = new HashMap<DatanodeID, DatanodeInfoDWRR>();
    this.nodeUuidtoNodeID = new HashMap<String, DatanodeID>();
    this.datanodeInfoComparator = new MarginalsComparatorDWRR();
    this.classInfoMap = new HashMap<Long, ClassInfoDWRR>();

    //this.threadedFairDWRR.start();
  }

  // TODO TODO sha daconseguir tambe la capacitat del datanode
  public void registerDatanode(DatanodeID datanodeID) {
    if (!this.nodeUuidtoNodeID.containsKey(datanodeID.getDatanodeUuid())) {
      DatanodeInfoDWRR datanode = new DatanodeInfoDWRR(datanodeID);
      this.nodeUuidtoNodeID.put(datanodeID.getDatanodeUuid(), datanodeID);
      this.nodeIDtoInfo.put(datanodeID, datanode);
    }

  }

  public boolean existsClassInfo(long classId) {
    return this.classToDatanodes.containsKey(new ClassInfoDWRR(classId));
  }

  public void setClassWeight(long classId) {
    setClassWeight(classId, FairIOControllerDWRR.DEFAULT_WEIGHT);
  }

  public void setClassWeight(long classId, float weight) {
    ClassInfoDWRR classInfo = new ClassInfoDWRR(classId, weight);
    Set<DatanodeInfoDWRR> datanodes = this.classToDatanodes.get(classInfo);
    if (datanodes == null)
      datanodes = new HashSet<DatanodeInfoDWRR>();
    this.classToDatanodes.put(classInfo, datanodes);
    this.classInfoMap.put(classId, classInfo);

    computeShares();
  }


  /* Create a datanode datatype identified with datanodeID to a given class */
	/* datanodeID should have been previously registered */
	/* classInfo should have been previously registered */
  public void addDatanodeToClass(long classId, String datanodeUUID) throws Exception {
    ClassInfoDWRR classInfo = new ClassInfoDWRR(classId);
    if (!this.nodeUuidtoNodeID.containsKey(datanodeUUID))
      throw new Exception("Node "+datanodeUUID+" not registered");

    DatanodeID datanodeID = this.nodeUuidtoNodeID.get(datanodeUUID);
    DatanodeInfoDWRR datanode = this.nodeIDtoInfo.get(datanodeID);

    this.classToDatanodes.get(classInfo).add(datanode);
    computeShares();
  }

  /* remove the datanode from a given class, put its weight to ZERO because
	 * not interested anymore */
	/* datanodeID should have been previously registered */
  public void removeDatanodeFromClass(long classId, String datanodeUUID) {
    ClassInfoDWRR classInfo = new ClassInfoDWRR(classId);
    if (this.classToDatanodes.containsKey(classInfo)) {
      DatanodeID datanodeID = this.nodeUuidtoNodeID.get(datanodeUUID);
      if (datanodeID != null) {
        DatanodeInfoDWRR datanode = this.nodeIDtoInfo.get(datanodeID);
        this.classToDatanodes.get(classInfo).remove(datanode);
        datanode.updateClassWeight(classInfo, BigDecimal.ZERO);
        // Remove the class from memory if no more datanodes
        if (this.classToDatanodes.size() == 0)
          this.classToDatanodes.remove(classInfo);
      }
    }
    computeShares();
  }

  public float getClassWeight(long classId) {
    return classInfoMap.get(classId).getWeight().floatValue();
  }


  public float getClassWeight(long classId, String dnuuid) {
    // TODO TODO controlar el cas que no existeixi, llavors preguntar per xattrs
    // aixo passa al apagar i tornar a encedre el sistema, ja que tot esta en memoria
    LOG.info("CAMAMILLA FairIOControllerDWRR.getClassWeight classId="+classId+" dnid="+dnuuid);        // TODO TODO log
    DatanodeID dnid = nodeUuidtoNodeID.get(dnuuid);
    DatanodeInfoDWRR info = nodeIDtoInfo.get(dnid);
    ClassInfoDWRR classInfo = new ClassInfoDWRR(classId);
    BigDecimal value = info.getClassWeight(classInfo);
    float ret = value.floatValue();
    return ret;
  }

  /* Compute the corresponding shares for all classids */
  public synchronized void computeShares() {
    HashMap<ClassInfoDWRR, BigDecimal> previousUtilities = new HashMap<ClassInfoDWRR, BigDecimal>();

    while (!isUtilityConverged(previousUtilities)) {
      for (ClassInfoDWRR classInfo : classToDatanodes.keySet()) {
        computeSharesByClass(classInfo);
      }

    }
  }

  @Deprecated
  public void initializeAllShares() {
    for (ClassInfoDWRR classInfo : classToDatanodes.keySet()) {
      initializeShares(classInfo);
    }
  }

  @Deprecated
  public void initializeShares(ClassInfoDWRR classInfo) {
    int numDatanodes = classToDatanodes.get(classInfo).size();
    for (DatanodeInfoDWRR datanode : classToDatanodes.get(classInfo)) {
      BigDecimal initWeight = classInfo.getWeight().divide(new BigDecimal(numDatanodes), CONTEXT);
      datanode.updateClassWeight(classInfo, initWeight);
    }
  }

  public String toString() {
    String res = "ClassInfoDWRR-DatanodeInfoDWRR Status\n";
    res += "-----------------------------\n";
    for (ClassInfoDWRR classInfo : classToDatanodes.keySet()) {
      res += "ClassInfoDWRR: "+classInfo+ "\n";
      for (DatanodeInfoDWRR datanode : classToDatanodes.get(classInfo)) {
        BigDecimal classWeight = datanode.getClassWeight(classInfo);
        BigDecimal classShare = datanode.getClassShare(classInfo);
        res += String.format("\t Datanode %s: %s %s\n", datanode, FairIOControllerDWRR.decimalFormat.format(classWeight), FairIOControllerDWRR.decimalFormat.format(classShare));
      }
    }
    return res;
  }

  private BigDecimal getUtility(ClassInfoDWRR classInfo) {
    BigDecimal utility = BigDecimal.ZERO;
    for (DatanodeInfoDWRR datanode : this.classToDatanodes.get(classInfo)) {
      BigDecimal cj = datanode.getCapacity();
      BigDecimal sij = datanode.getClassShare(classInfo);
      // sum_ut += disk.capacity * disk.get_share_byfile(fid)
      utility = utility.add(cj.multiply(sij));
    }
    return utility;
  }

  private Map<ClassInfoDWRR, BigDecimal> getUtilities() {
    HashMap<ClassInfoDWRR, BigDecimal> utilities = new HashMap<ClassInfoDWRR, BigDecimal>();
    for (ClassInfoDWRR classInfo : classToDatanodes.keySet()) {
      utilities.put(classInfo, getUtility(classInfo));
    }
    return utilities;
  }

  /* Return wether the current utility of all classes differ less than
	 * min utility gap wrt previous utility.
	 */
  private boolean isUtilityConverged(Map<ClassInfoDWRR, BigDecimal> previousUtilities) {
    boolean converged = true;
    Map<ClassInfoDWRR, BigDecimal> currentUtilities = getUtilities();

    if (currentUtilities.isEmpty())
      return true;

    // no previous utilities, so update with current ones and return not converged
    if (previousUtilities.isEmpty()) {
      previousUtilities.putAll(currentUtilities);
      return false;
    }

    // Use current utilities to compare with previousUtilities
    for (ClassInfoDWRR classInfo : currentUtilities.keySet()) {
      BigDecimal currentUtility = currentUtilities.get(classInfo);
      BigDecimal previousUtility = previousUtilities.get(classInfo);
      BigDecimal utilityGap = currentUtility.subtract(previousUtility).abs();
      if (utilityGap.compareTo(MIN_UTILITY_GAP) <= 0) {
        converged = converged && true;
      }
      else {
        converged = false;
      }
    }
    previousUtilities.clear();
    previousUtilities.putAll(currentUtilities);
    return converged;
  }

  private void computeSharesByClass(ClassInfoDWRR classInfo) {
    ArrayList<DatanodeInfoDWRR> datanodes = new ArrayList<DatanodeInfoDWRR>(this.classToDatanodes.get(classInfo));
    Collections.sort(datanodes, this.datanodeInfoComparator);

    // Optimization algorithm per se
    //sub_total_mins = sum([yj*min_coeff for _, _, _, yj, _, _ in marginals])
    BigDecimal sub_total_mins = BigDecimal.ZERO;
    for (DatanodeInfoDWRR datanode : datanodes) {
      BigDecimal yj = datanode.getTotalWeight();
      sub_total_mins = sub_total_mins.add(yj.multiply(MIN_COEFF));
    }
    BigDecimal budget = classInfo.getWeight();
    BigDecimal sub_total_sqrt_wy = BigDecimal.ZERO;
    BigDecimal sub_total_y = BigDecimal.ZERO;
    BigDecimal last_coeff = BigDecimal.ZERO;
    int k = 0;

    for (DatanodeInfoDWRR datanode : datanodes) {
      BigDecimal yj = datanode.getTotalWeight(); // total weights on this datanode, price
      BigDecimal cj = datanode.getCapacity(); // capacity for this datanode
      // sqrt_wy = (yj * cj).sqrt()
      BigDecimal sqrt_wy = sqrt(yj.multiply(cj));
      // sub_total_sqrt_wy += sqrt_wy;
      sub_total_sqrt_wy = sub_total_sqrt_wy.add(sqrt_wy);
      // sub_total_y += yj;
      sub_total_y = sub_total_y.add(yj);
      // sub_total_mins -= yj*min_coeff
      sub_total_mins = sub_total_mins.subtract(yj.multiply(MIN_COEFF));
      // coeff = (budget - sub_total_mins + sub_total_y) / sub_total_sqrt_wy;
      BigDecimal coeff = budget.subtract(sub_total_mins)
        .add(sub_total_y)
        .divide(sub_total_sqrt_wy, CONTEXT);
      // t = (sqrt_wy * coeff) - yj
      BigDecimal t = sqrt_wy.multiply(coeff).subtract(yj);
      //tmin = t - ((min_share*yj / (1 - min_share))
      BigDecimal tmin = t.subtract(MIN_SHARE.multiply(yj)
        .divide(ONE_MINUS_MIN_SHARE, CONTEXT));
      // if tmin >= 0
      if (tmin.compareTo(BigDecimal.ZERO) >= 0) {
        k++;
        last_coeff = coeff;
      }
      else
        break;
    }

    // Update weight on each node with xij higher than min_coeff
    for (int i = 0; i < k; i++) {
      DatanodeInfoDWRR datanode = datanodes.get(i);
      BigDecimal yj = datanode.getTotalWeight();
      BigDecimal cj = datanode.getCapacity();
      // xij = ((yj*cj).sqrt() * last_coeff) - yj
      BigDecimal xij = sqrt(yj.multiply(cj))
        .multiply(last_coeff)
        .subtract(yj);
      datanode.updateClassWeight(classInfo, xij);
    }
    // Update the rest of nodes with an xij = min_coeff
    for (int i = k; i < datanodes.size(); i++) {
      DatanodeInfoDWRR datanode = datanodes.get(i);
      BigDecimal yj = datanode.getTotalWeight();
      // xij = yj * min_coeff
      BigDecimal xij = yj.multiply(MIN_COEFF);
      datanode.updateClassWeight(classInfo, xij);
    }
  }

  private static BigDecimal sqrt(BigDecimal A) {
    BigDecimal x0 = new BigDecimal(0, CONTEXT);
    BigDecimal x1 = new BigDecimal(Math.sqrt(A.doubleValue()), CONTEXT);
    while (!x0.equals(x1)) {
      x0 = x1;
      x1 = A.divide(x0, CONTEXT);
      x1 = x1.add(x0);
      x1 = x1.divide(TWO, CONTEXT);

    }
    return x1;
  }

}

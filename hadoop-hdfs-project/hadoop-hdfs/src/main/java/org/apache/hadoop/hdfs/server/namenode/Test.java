package org.apache.hadoop.hdfs.server.namenode;

import java.util.HashSet;

/**
 * Created by DEIM on 21/10/14.
 */
public class Test {


  public static void main(String[] args) {
    ClassInfoDWRR cl = new ClassInfoDWRR(100);
    ClassInfoDWRR cl2 = new ClassInfoDWRR(100);

    HashSet<ClassInfoDWRR> set = new HashSet<ClassInfoDWRR>();
    System.out.println("Abans "+set.contains(cl2));
    set.add(cl);
    System.out.println("DEspres " + set.contains(cl2));

  }
}

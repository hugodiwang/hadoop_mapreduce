package com.muke.topn;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TGroupingComputer extends WritableComparator {
  public TGroupingComputer(){
    super(TKey.class, true);
  }

  public int compare(WritableComparable a, WritableComparable b){
    TKey k1 = (TKey)a;
    TKey k2 = (TKey)b;
    int c1 = Integer.compare(k1.getYear(), k2.getYear());
    if(c1 == 0){
      return  Integer.compare(k1.getMonth(), k2.getMonth());

    }
    return c1;
  }


}

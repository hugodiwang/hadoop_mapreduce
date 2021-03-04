package com.muke.topn;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TSortComparator extends WritableComparator {
  //when buffer reaches 80% , do sort and then do write desk
  //it first compare byte[] then compare tkey
  // override tkey.compare
  // assign class deal with inner comparable in exteds
  public TSortComparator(){
    super(TKey.class, true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b){
    TKey k1 = (TKey)a;
    TKey k2 = (TKey)b;
    int c1 = Integer.compare(k1.getYear(), k2.getYear());
    if(c1 == 0){
      int c2 = Integer.compare(k1.getMonth(), k2.getMonth());
      if(c2==0){
        return - Integer.compare(k1.getTemp(), k2.getTemp());
      }
      return c2;
    }
    return c1;
  }


}

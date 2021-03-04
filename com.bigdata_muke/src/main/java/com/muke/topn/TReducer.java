package com.muke.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TReducer extends Reducer<TKey, IntWritable, Text, IntWritable> {

  Text rkey = new Text();
  IntWritable rval = new IntWritable();
  protected void reduce(TKey key, Iterable<IntWritable> value,
                        Context context) throws IOException, InterruptedException {
  // 1970-6-4 33 33
  // 1970-6-4 32 32
  // 1970-6-1 32 32
  // we donot need values here, when we iter values
  // Iterator<IntWritable> iter = values.iterator();
  // while(iter.hasNext()){
  //    IntWritable val = iter.next();
  // iter.next ---> context.nextKeyValue() update key, value
    Iterator<IntWritable> iter = value.iterator();
    int flag = 0;
    int day = 0;
    while(iter.hasNext()){
      IntWritable val = iter.next();
      if(flag == 0){
        rkey.set(key.getYear() + "-" + key.getMonth() +  "-" + key.getDay());
        rval.set(key.getDay());
        context.write(rkey, rval);
        flag ++;
        day = key.getDay();
      }
      if(flag != 0 && day!= key.getDay()){
        rkey.set(key.getYear() + "-" + key.getMonth() +  "-" + key.getDay());
        rval.set(key.getDay());
        context.write(rkey, rval);
        break;
      }
    }

  }
}

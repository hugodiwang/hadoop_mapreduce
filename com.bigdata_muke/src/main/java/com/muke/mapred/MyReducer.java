package com.muke.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  private IntWritable result = new IntWritable();
  // one group with the same key
  // value of the same group
  public void reduce(Text key, Iterable<IntWritable> value,
                     Context context) throws IOException, InterruptedException {
    int sum = 0;
    for(IntWritable val : value){
      sum += val.get();
    }
    result.set(sum);
    context.write(key, result);
  }
}

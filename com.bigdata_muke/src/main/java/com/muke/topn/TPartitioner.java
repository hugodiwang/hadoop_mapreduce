package com.muke.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TPartitioner extends Partitioner<TKey, IntWritable> {
  @Override
  public int getPartition(TKey key, IntWritable value, int numPartitions) {
    // 1 simple
    // 2 bigger than group the same group with the same partition id
    return key.getYear() % numPartitions;


  }
}

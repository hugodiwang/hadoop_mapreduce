package com.muke.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
  // data is serialized and deserialized
  // hadoop mapping string to text, int to intwritable
  // if customized, must implement serialized and deserialized/ sort
  // sort ---> dict, numerical

  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();

  // run itself do init context, map and close context at each run
  // key is offset from the start of the file and value is the content

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString());
    while(itr.hasMoreTokens()){
      word.set(itr.nextToken());
      // write do serialization so we do not need to worry about the reference copy
      // if we put new here, we do gc frequently which may cause full gc
      context.write(word, one);

    }

  }
}

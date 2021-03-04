package com.muke.topn;

import com.muke.mapred.MyMapper;
import com.muke.mapred.TestMapRed;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class MyTopN {
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    // conf parse .xml
    Configuration conf = new Configuration(true);
    // for spliting args
    String[] other = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = Job.getInstance(conf);

    //for package using from class mapping to java package
    job.setJarByClass(MyTopN.class);
    job.setJobName("topn");

    //set config and client main code parts
    //maptask
    //input
    TextInputFormat.addInputPath(job, new Path(other[0]));

    //output
    Path outPath = new Path(other[1]);
    if(outPath.getFileSystem(conf).exists(outPath)) {
      outPath.getFileSystem(conf).delete(outPath, true);
    }
    TextOutputFormat.setOutputPath(job, outPath);

    //key class
    //map
    //todo
    job.setMapperClass(TMapper.class);
    // from map to reduce, indicate the class type reflect
    job.setMapOutputKeyClass(TKey.class);
    job.setMapOutputValueClass(IntWritable.class);

    //partitioner based on year month
    job.setPartitionerClass(TPartitioner.class);

    //sortComparator
    job.setSortComparatorClass(TSortComparator.class);
    //combine  before write desk

    //reducetask
    //groupingComparator
    job.setGroupingComparatorClass(TGroupingComputer.class);
    //reduce
    job.setReducerClass(TReducer.class);

    // true for outputing
    job.waitForCompletion(true);
  }


}

package com.muke.mapred;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class TestMapRed {

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    // conf parse .xml
    Configuration conf = new Configuration(true);
    Job job = Job.getInstance(conf);
    //for package using from class mapping to java package
    job.setJarByClass(TestMapRed.class);
    job.setJobName("mr_wordcount");

    // text or db data source and then do reformat
    Path infile = new Path("/root/input");
    TextInputFormat.addInputPath(job, infile);

    Path outfile = new Path("/root/output");
    // fs = outfile.getFileSystem(conf)
    if(outfile.getFileSystem(conf).exists(outfile)) {
      outfile.getFileSystem(conf).delete(outfile, true);
    }
    TextOutputFormat.setOutputPath(job, outfile);

    //todo
    job.setMapperClass(MyMapper.class);
    // from map to reduce, indicate the class type reflect
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    //todo
    job.setReducerClass(MyReducer.class);

    // after setting the config, submit
    job.waitForCompletion(true);

  }
}

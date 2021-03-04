package com.muke.hdfs;

import com.sun.jersey.json.impl.BufferingInputOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.nio.Buffer;


// C:\Windows\System32\drivers\etc
// host mapping is required
public class TestHDFS {
  public Configuration conf = null;
  public FileSystem fs = null;

  @Before
  public void conn() throws IOException, InterruptedException {
    // true load local .xml
    conf = new Configuration(true);
    // way1
    // use get(conf) needs to set env HADOOP_USER_NAME = root
    //
    fs = FileSystem.get(conf);
    // abstract father class get child class based on .xml

    //way 2
    fs = FileSystem.get(URI.create("hdfs://mycluster/"), conf, "root");
  }

  @Test
  public void mkdir() throws IOException {
    Path dir = new Path("/hdfs");
    if(fs.exists(dir)){
      fs.delete(dir, true);
    }
    fs.mkdirs(dir);
  }

  @Test
  public void upload() throws IOException {
    BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File("./data/hello.txt")));
    Path outfile = new Path("/hdfs/out.text");
    FSDataOutputStream output = fs.create(outfile);
    IOUtils.copyBytes(input, output, conf, true);
  }

  @Test
  public void blocks() throws IOException {
    Path file = new Path("/data.txt");
    FileStatus  fss = fs.getFileStatus(file);
    BlockLocation[] blks = fs.getFileBlockLocations(fss, 0, fss.getLen());
    for (BlockLocation b : blks){
      System.out.println(b);
    }
    // return
    // start , end, node
  }

  @After
  public void close(){

  }


}

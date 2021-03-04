package com.muke.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


import static java.util.Calendar.YEAR;

public class TMapper extends Mapper<LongWritable, Text, TKey, IntWritable> {
  TKey mkey = new TKey();
  IntWritable mval = new IntWritable();


  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
  // value: 2018-01-03 22:10:10 1 31
    String[] strs = StringUtils.split(value.toString(), '\t');
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    try{
      Date date = sdf.parse(strs[0]);
      Calendar cal = Calendar.getInstance();
      cal.setTime(date);
      mkey.setYear(cal.get(Calendar.YEAR));
      mkey.setMonth(cal.get(Calendar.MONTH-1));
      mkey.setDay(cal.get(Calendar.DAY_OF_MONTH));
      mkey.setTemp(Integer.parseInt(strs[2]));
      mval.set(Integer.parseInt(strs[2]));
    } catch (ParseException e){
      e.printStackTrace();
    }

  }
}

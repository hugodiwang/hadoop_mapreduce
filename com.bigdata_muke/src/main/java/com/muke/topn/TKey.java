package com.muke.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// serialization, compartor
public class TKey implements WritableComparable<TKey> {
  private int year;

  public int getYear() {
    return year;
  }

  public void setYear(int year) {
    this.year = year;
  }

  public int getMonth() {
    return month;
  }

  public void setMonth(int month) {
    this.month = month;
  }

  public int getDay() {
    return day;
  }

  public void setDay(int day) {
    this.day = day;
  }

  public int getTemp() {
    return temp;
  }

  public void setTemp(int temp) {
    this.temp = temp;
  }

  private int month;
  private int day;
  private int temp;

  @Override
  public int compareTo(TKey that) {
    //for normally using, we implement the asc order here
    int c1 = Integer.compare(this.year, that.year);
    if(c1 == 0){
      int c2 = Integer.compare(this.month, that.month);
      if(c2 == 0){
        return Integer.compare(this.day, that.day);
      }
      return c2;

    }

    return c1;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(year);
    out.writeInt(month);
    out.writeInt(day);
    out.writeInt(temp);

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.year = in.readInt();
    this.month = in.readInt();
    this.day = in.readInt();
    this.temp = in.readInt();

  }
}

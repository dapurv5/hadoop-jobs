/**
 *  Copyright (c) 2016 Apurv Verma
 */
package com.apurv.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class LongText implements WritableComparable<LongText>{
  private LongWritable first;
  private Text second;
  
  public LongText(){
    first = new LongWritable();
    second = new Text();
  }
  
  public void set(long a, String b){
    first.set(a);
    second.set(b);
  }
  
  public long getFirst() {
    return first.get();
  }

  public String getSecond() {
    return second.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first.readFields(in);
    second.readFields(in);
  }
  
  @Override
  public String toString(){
    return first.get() + ","+second.toString();
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((first == null) ? 0 : first.hashCode());
    result = prime * result + ((second == null) ? 0 : second.toString().hashCode());
    return result;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    LongText other = (LongText) obj;
    if (first == null) {
      if (other.first != null)
        return false;
    } else if (!first.equals(other.first))
      return false;
    if (second == null) {
      if (other.second != null)
        return false;
    } else if (!second.equals(other.second))
      return false;
    return true;
  }

  @Override
  public int compareTo(LongText that) {
    int cmp = first.compareTo(that.first);
    if(cmp != 0){
      return cmp;
    }
    return second.compareTo(that.second);
  }
}

package com.joefkelley.matrix.mr

import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.IntWritable
import java.io.DataOutput
import java.io.DataInput
import org.apache.hadoop.io.WritableComparable

class IntTripleWritable(
    a: IntWritable = new IntWritable,
    b: IntWritable = new IntWritable,
    c: IntWritable = new IntWritable
    ) extends Tuple3[IntWritable, IntWritable, IntWritable](a, b, c) with WritableComparable[IntTripleWritable] {
  
  // needed because hadoop calls it via reflection
  def this() = this(new IntWritable, new IntWritable, new IntWritable)
  
  override def readFields(in: DataInput): Unit = {
    _1.readFields(in)
    _2.readFields(in)
    _3.readFields(in)
  }
  
  override def write(out: DataOutput): Unit = {
    _1.write(out)
    _2.write(out)
    _3.write(out)
  }
  
  override def compareTo(other: IntTripleWritable): Int = {
    val x = _1.compareTo(other._1)
    if (x == 0) {
      val y = _2.compareTo(other._2)
      if (y == 0) {
        _3.compareTo(other._3)
      } else {
        y
      }
    } else {
      x
    }
  }
  
}
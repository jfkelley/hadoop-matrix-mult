package com.joefkelley.matrix.mr

import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import java.io.DataOutput
import java.io.DataInput

class MatrixElementWritable(
    val row: Text = new Text,
    val col: Text = new Text,
    val value: DoubleWritable = new DoubleWritable,
    val isLeft: BooleanWritable = new BooleanWritable) extends Writable {
  
  // needed because hadoop calls it via reflection
  def this() = this(new Text, new Text, new DoubleWritable, new BooleanWritable)
  
  override def write(out: DataOutput): Unit = {
    row.write(out)
    col.write(out)
    value.write(out)
    isLeft.write(out)
  }
  
  override def readFields(in: DataInput): Unit = {
    row.readFields(in)
    col.readFields(in)
    value.readFields(in)
    isLeft.readFields(in)
  }
  
}
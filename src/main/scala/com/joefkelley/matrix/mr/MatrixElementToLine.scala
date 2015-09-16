package com.joefkelley.matrix.mr

import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.conf.Configured

trait MatrixElementToLine extends Configured {
  
  def setLine(row: Text, col: Text, value: DoubleWritable, line: Text): Unit
  
}
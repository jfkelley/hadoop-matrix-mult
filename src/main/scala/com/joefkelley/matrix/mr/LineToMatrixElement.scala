package com.joefkelley.matrix.mr

import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.conf.Configured

trait LineToMatrixElement extends Configured {
  
  def setRowColValue(line: Text, row: Text, col: Text, value: DoubleWritable): Unit
  
}
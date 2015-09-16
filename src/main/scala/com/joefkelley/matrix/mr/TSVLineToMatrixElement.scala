package com.joefkelley.matrix.mr

import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.TaskAttemptContext

class TSVRowToMatrixElement extends LineToMatrixElement {
  
  def setRowColValue(line: Text, row: Text, col: Text, value: DoubleWritable): Unit = {
    line.toString().split("\t") match {
      case Array(r, c, v) => {
        row.set(r)
        col.set(c)
        value.set(v.toDouble)
      }
    }
  }
  
}
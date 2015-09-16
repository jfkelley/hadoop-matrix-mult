package com.joefkelley.matrix.mr

import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text

class TSVMatrixElementToLine extends MatrixElementToLine {

  def setLine(row: Text, col: Text, value: DoubleWritable, line: Text): Unit = {
    line.set(s"${row.toString}\t${col.toString}\t${value.get()}")
  }
  
}
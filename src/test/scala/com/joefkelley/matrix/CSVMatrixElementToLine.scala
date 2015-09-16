package com.joefkelley.matrix

import com.joefkelley.matrix.mr.MatrixElementToLine
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text

class CSVMatrixElementToLine extends MatrixElementToLine {

  def setLine(row: Text, col: Text, value: DoubleWritable, line: Text): Unit = {
    line.set(s"${row.toString},${col.toString},${value.get()}")
  }
  
}
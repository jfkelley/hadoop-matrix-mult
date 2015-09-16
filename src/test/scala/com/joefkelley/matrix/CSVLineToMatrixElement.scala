package com.joefkelley.matrix

import org.apache.hadoop.io.DoubleWritable
import com.joefkelley.matrix.mr.LineToMatrixElement
import org.apache.hadoop.io.Text

class CSVLineToMatrixElement extends LineToMatrixElement {
  
  def setRowColValue(line: Text, row: Text, col: Text, value: DoubleWritable): Unit = {
    line.toString().split(",") match {
      case Array(r, c, v) => {
        row.set(r)
        col.set(c)
        value.set(v.toDouble)
      }
    }
  }
  
}
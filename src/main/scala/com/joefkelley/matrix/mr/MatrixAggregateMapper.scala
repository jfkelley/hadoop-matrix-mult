package com.joefkelley.matrix.mr

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text

class MatrixAggregateMapper extends Mapper[NullWritable, MatrixElementWritable, Text, DoubleWritable] {
  
  private type Context = Mapper[NullWritable, MatrixElementWritable, Text, DoubleWritable]#Context
  
  val outputKey = new Text()
  
  override def map(ignored: NullWritable, element: MatrixElementWritable, context: Context): Unit = {
    outputKey.set(element.row + "\t" + element.col)
    context.write(outputKey, element.value)
  }

}
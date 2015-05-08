package com.joefkelley.matrix.mr

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class MatrixAggregateCombiner extends Reducer[Text, DoubleWritable, Text, DoubleWritable] {
  
  private type Context = Reducer[Text, DoubleWritable, Text, DoubleWritable]#Context
  
  val outputValue = new DoubleWritable()
  
  override def reduce(key: Text, vals: java.lang.Iterable[DoubleWritable], context: Context): Unit = {
    import scala.collection.JavaConversions.iterableAsScalaIterable
    val sum = vals.map(_.get).sum
    outputValue.set(sum)
    context.write(key, outputValue)
  }

}
package com.joefkelley.matrix.mr

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.DoubleWritable

class MatrixAggregateReducer extends Reducer[Text, DoubleWritable, Text, NullWritable] {
  
  private type Context = Reducer[Text, DoubleWritable, Text, NullWritable]#Context
  
  val outputKey = new Text
  
  override def reduce(key: Text, vals: java.lang.Iterable[DoubleWritable], context: Context): Unit = {
    import scala.collection.JavaConversions.iterableAsScalaIterable
    val sum = vals.map(_.get).sum
    outputKey.set(key + "\t" + sum)
    context.write(outputKey, NullWritable.get)
  }

}
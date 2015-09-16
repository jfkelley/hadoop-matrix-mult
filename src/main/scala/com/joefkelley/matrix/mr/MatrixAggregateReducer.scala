package com.joefkelley.matrix.mr

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.DoubleWritable
import com.joefkelley.matrix.MRMultiply

class MatrixAggregateReducer extends Reducer[Text, DoubleWritable, Text, NullWritable] {
  
  private type Context = Reducer[Text, DoubleWritable, Text, NullWritable]#Context
  
  val row = new Text
  val col = new Text
  val value = new DoubleWritable
  val outputKey = new Text
  
  var outputConverter: MatrixElementToLine = _
  
  override def setup(context: Context): Unit = {
    val outputConverterClass = context.getConfiguration().getClass(MRMultiply.OUTPUT_CONVERTER_KEY, classOf[TSVMatrixElementToLine], classOf[MatrixElementToLine])
    outputConverter = outputConverterClass.newInstance()
    outputConverter.setConf(context.getConfiguration)
  }
  
  override def reduce(key: Text, vals: java.lang.Iterable[DoubleWritable], context: Context): Unit = {
    import scala.collection.JavaConversions.iterableAsScalaIterable
    val sum = vals.map(_.get).sum
    val keyStr = key.toString()
    val Array(r, c) = keyStr.split('\t')
    row.set(r)
    col.set(c)
    value.set(sum)
    outputConverter.setLine(row, col, value, outputKey)
    context.write(outputKey, NullWritable.get)
  }

}
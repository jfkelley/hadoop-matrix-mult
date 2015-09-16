package com.joefkelley.matrix.mr

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import com.joefkelley.matrix.MRMultiply

class MatrixInputFormat(isLeft: Boolean) extends FileInputFormat[NullWritable, MatrixElementWritable] {
  
  private val textInput = new TextInputFormat()
  
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[NullWritable, MatrixElementWritable] = {
    val textRecordReader = textInput.createRecordReader(split, context)
    val mElement = new MatrixElementWritable()
    mElement.isLeft.set(isLeft)
    
    val inputConverterClass = context.getConfiguration().getClass(MRMultiply.INPUT_CONVERTER_KEY, classOf[TSVRowToMatrixElement], classOf[LineToMatrixElement])
    val inputConverter = inputConverterClass.newInstance()
    inputConverter.setConf(context.getConfiguration())
    
    new MappedRecordReader[LongWritable, Text, NullWritable, MatrixElementWritable](
        textRecordReader,
        NullWritable.get,
        mElement,
        (_, _, n) => n,
        (_, line, m) => {
          inputConverter.setRowColValue(line, m.row, m.col, m.value)
          m
        })
  }
  
}
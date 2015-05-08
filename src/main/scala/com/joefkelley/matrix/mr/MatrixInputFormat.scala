package com.joefkelley.matrix.mr

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

class MatrixInputFormat(isLeft: Boolean) extends FileInputFormat[NullWritable, MatrixElementWritable] {
  
  private val textInput = new TextInputFormat()
  
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[NullWritable, MatrixElementWritable] = {
    val textRecordReader = textInput.createRecordReader(split, context)
    val mElement = new MatrixElementWritable()
    mElement.isLeft.set(isLeft)
    new MappedRecordReader[LongWritable, Text, NullWritable, MatrixElementWritable](
        textRecordReader,
        NullWritable.get,
        mElement,
        (_, _, n) => n,
        (_, line, m) => {
          line.toString().split('\t') match { case Array(r, c, v) =>
            m.row.set(r)
            m.col.set(c)
            m.value.set(v.toDouble)
            m
          }
        })
  }
  
}
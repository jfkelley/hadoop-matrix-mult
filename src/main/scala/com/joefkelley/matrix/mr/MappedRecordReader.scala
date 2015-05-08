package com.joefkelley.matrix.mr

import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.TaskAttemptContext

class MappedRecordReader[KEYIN, VALIN, KEYOUT, VALOUT](
    private val wrapped: RecordReader[KEYIN, VALIN],
    private var currentKey: KEYOUT,
    private var currentVal: VALOUT,
    private val keyFunction: (KEYIN, VALIN, KEYOUT) => KEYOUT,
    private val valFunction: (KEYIN, VALIN, VALOUT) => VALOUT) extends RecordReader[KEYOUT, VALOUT] {
  
  def close(): Unit = wrapped.close()
  def getCurrentKey(): KEYOUT = currentKey
  def getCurrentValue(): VALOUT = currentVal
  def getProgress(): Float = wrapped.getProgress()
  def initialize(split: InputSplit, context: TaskAttemptContext): Unit = wrapped.initialize(split, context)
  
  def nextKeyValue(): Boolean = {
    if (wrapped.nextKeyValue()) {
      currentKey = keyFunction(wrapped.getCurrentKey, wrapped.getCurrentValue, currentKey)
      currentVal = valFunction(wrapped.getCurrentKey, wrapped.getCurrentValue, currentVal)
      true
    } else {
      false
    }
  }

}
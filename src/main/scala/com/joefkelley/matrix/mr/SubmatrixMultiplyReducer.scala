package com.joefkelley.matrix.mr

import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import com.joefkelley.matrix.CompressedMatrixMultiply
import com.joefkelley.matrix.MatrixBuilder
import com.joefkelley.matrix.MatrixElement
import com.joefkelley.matrix.MRMultiply

class SubmatrixMultiplyReducer extends Reducer[IntTripleWritable, MatrixElementWritable, Text, NullWritable] {
  
  private type Context = Reducer[IntTripleWritable, MatrixElementWritable, Text, NullWritable]#Context
  
  val outputKey = new Text
  
  var oneMR = false
  val cache = scala.collection.mutable.Map.empty[(String, String), Double]
  
  override def setup(context: Context): Unit = {
    oneMR = context.getConfiguration().getBoolean(MRMultiply.ONE_MR_CONF_FLAG, false)
  }
  
  override def cleanup(context: Context): Unit = {
    for (((row, col), value) <- cache) {
      output(row, col, value, context)
    }
  }
  
  override def reduce(key: IntTripleWritable, vals: java.lang.Iterable[MatrixElementWritable], context: Context): Unit = {
    import scala.collection.JavaConversions.iterableAsScalaIterable
    val leftEdges = new ArrayBuffer[(String, String, Double)]
    val rightEdges = new ArrayBuffer[(String, String, Double)]
    for (element <- vals) {
      val extracted = (element.row.toString, element.col.toString, element.value.get)
      if (element.isLeft.get) {
        leftEdges += extracted
      } else {
        rightEdges += extracted
      }
    }
    
    val intersection = leftEdges.map(_._2).toSet & rightEdges.map(_._1).toSet
    val leftIncludedEdges = leftEdges.filter(e => intersection.contains(e._2))
    val rightIncludedEdges = rightEdges.filter(e => intersection.contains(e._1))
    
    val leftRows = leftIncludedEdges.map(_._1).toSet.toVector
    val leftCols = intersection.toVector
    val rightRows = leftCols
    val rightCols = rightIncludedEdges.map(_._2).toSet.toVector
    
    val leftRowMap = leftRows.zipWithIndex.toMap
    val leftColMap = leftCols.zipWithIndex.toMap
    val rightRowMap = leftColMap
    val rightColMap = rightCols.zipWithIndex.toMap
    
    val left = new MatrixBuilder(leftIncludedEdges.map{
      case (r, c, v) => new MatrixElement(leftRowMap(r), leftColMap(c), v)
    }).buildCompressedRowStorage()
    
    val right = new MatrixBuilder(rightIncludedEdges.map{
      case (r, c, v) => new MatrixElement(rightRowMap(r), rightColMap(c), v)
    }).buildCompressedColStorage()
    
    val result = CompressedMatrixMultiply.multiply(left, right)
    
    for (element <- result) {
      val row = leftRows(element.row)
      val col = rightCols(element.col)
      val value = element.value
      if (oneMR) {
        updateCache(row, col, value)
      } else {
        output(row, col, value, context)
      }
    }
  }
  
  private[this] def output(row: String, col: String, value: Double, context: Context) {
    val asTsv = s"$row\t$col\t$value"
    outputKey.set(asTsv)
    context.write(outputKey, NullWritable.get)
  }
  
  private[this] def updateCache(row: String, col: String, value: Double) {
    cache.update((row, col), cache.getOrElse((row, col), 0.0) + value)
  }
  
}
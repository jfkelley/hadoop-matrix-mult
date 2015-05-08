package com.joefkelley.matrix.mr

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import scala.collection.mutable.ArrayBuffer
import com.joefkelley.matrix.MatrixBuilder
import com.joefkelley.matrix.MatrixElement
import com.joefkelley.matrix.CompressedMatrixMultiply

class SubmatrixMultiplyReducer extends Reducer[IntTripleWritable, MatrixElementWritable, Text, NullWritable] {
  
  private type Context = Reducer[IntTripleWritable, MatrixElementWritable, Text, NullWritable]#Context
  
  val outputKey = new Text
  
  override def reduce(key: IntTripleWritable, vals: java.lang.Iterable[MatrixElementWritable], context: Context): Unit = {
    System.out.println("Starting reduce key: " + key)
    import scala.collection.JavaConversions.iterableAsScalaIterable
    val leftEdges = new ArrayBuffer[(String, String, Double)]
    val rightEdges = new ArrayBuffer[(String, String, Double)]
    for (element <- vals) {
      val extracted = (element.row.toString, element.col.toString, element.value.get)
      if (element.isLeft.get) {
        System.out.println(s"Reducer received left element: $extracted")
        leftEdges += extracted
      } else {
        System.out.println(s"Reducer received right element: $extracted")
        rightEdges += extracted
      }
    }
    
    val intersection = leftEdges.map(_._2).toSet & rightEdges.map(_._1).toSet
    System.out.println(s"Intersection of keys = $intersection")
    val leftIncludedEdges = leftEdges.filter(e => intersection.contains(e._2))
    System.out.println(s"Left included edges = $leftIncludedEdges")
    val rightIncludedEdges = rightEdges.filter(e => intersection.contains(e._1))
    System.out.println(s"Right included edges = $rightIncludedEdges")
    
    val leftRows = leftIncludedEdges.map(_._1).toSet.toVector
    System.out.println(s"Left rows = $leftRows")
    val leftCols = intersection.toVector
    System.out.println(s"Left cols = $leftCols")
    val rightRows = leftCols
    val rightCols = rightIncludedEdges.map(_._2).toSet.toVector
    System.out.println(s"Right cols = $rightCols")
    
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
      val asTsv = s"${leftRows(element.row)}\t${rightCols(element.col)}\t${element.value}"
      System.out.println(s"outputting: $asTsv")
      outputKey.set(asTsv)
      context.write(outputKey, NullWritable.get)
    }
  }
  
}
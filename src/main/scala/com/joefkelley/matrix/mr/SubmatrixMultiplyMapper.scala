package com.joefkelley.matrix.mr

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Mapper

class SubmatrixMultiplyMapper extends Mapper[NullWritable, MatrixElementWritable, IntTripleWritable, MatrixElementWritable] {
  
  private type Context = Mapper[NullWritable, MatrixElementWritable, IntTripleWritable, MatrixElementWritable]#Context
  
  var nDivs: Int = 0;
  
  override def setup(context: Context): Unit = {
    nDivs = context.getConfiguration.getInt("N_SUBMATRIX_DIVS", 10)
  }
  
  override def map(ignored: NullWritable, value: MatrixElementWritable, context: Context): Unit = {
    val key = new IntTripleWritable
    val sr = hashMod(value.row.toString(), nDivs)
    val sc = hashMod(value.col.toString(), nDivs)
    if (value.isLeft.get) {
      key._1.set(sr)
      key._2.set(sc)
      for (i <- 0 until nDivs) {
        key._3.set(i)
        context.write(key, value)
      }
    } else {
      key._2.set(sr)
      key._3.set(sc)
      for (i <- 0 until nDivs) {
        key._1.set(i)
        context.write(key, value)
      }
    }
  }
  
  def hashMod(x: Any, m: Int): Int = {
    Math.abs(x.hashCode()) % m
  }

}
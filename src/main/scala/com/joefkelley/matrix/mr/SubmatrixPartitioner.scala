package com.joefkelley.matrix.mr

import org.apache.hadoop.mapreduce.Partitioner
import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.conf.Configuration

class SubmatrixPartitioner extends Partitioner[IntTripleWritable, MatrixElementWritable] with Configurable {
  
  private var conf: Configuration = null
  private var nDivs: Int = 10
  
  def getConf(): Configuration = conf
  def setConf(conf: Configuration): Unit = {
    this.conf = conf
    nDivs = conf.getInt("N_SUBMATRIX_DIVS", 10)
  }
  
  // Partition evenly, and such that combiner in next phase is effective...
  // submatrix products with the same i, k should tend to same reducer
  override def getPartition(key: IntTripleWritable, value: MatrixElementWritable, nReduceTasks: Int): Int = {
    val (i, j, k) = (key._1.get, key._2.get, key._3.get)
    val x = i * nDivs * nDivs + k * nDivs + j
    x * nReduceTasks / (nDivs * nDivs * nDivs)
  }
  
}
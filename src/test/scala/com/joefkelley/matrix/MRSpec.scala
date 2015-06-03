package com.joefkelley.matrix

import org.scalatest._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.joefkelley.matrix.mr.MatrixElementWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.BooleanWritable
import com.joefkelley.matrix.mr.SubmatrixMultiplyMapper
import com.joefkelley.matrix.mr.SubmatrixMultiplyReducer
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver
import org.apache.hadoop.io.NullWritable
import scala.collection.JavaConversions._
import com.joefkelley.matrix.mr.MatrixAggregateMapper
import com.joefkelley.matrix.mr.MatrixAggregateReducer
import com.joefkelley.matrix.mr.MatrixAggregateCombiner
import com.joefkelley.matrix.mr.SubmatrixPartitioner
import org.apache.hadoop.conf.Configuration
import com.joefkelley.matrix.mr.IntTripleWritable
import org.apache.hadoop.io.IntWritable

@RunWith(classOf[JUnitRunner])
class MRSpec extends FlatSpec with Matchers {
  
  val input = SampleMatrixes.A.map(e =>
    new MatrixElementWritable(
      new Text(e.row.toString),
      new Text(e.col.toString),
      new DoubleWritable(e.value),
      new BooleanWritable(true)
    )
  ) ++ SampleMatrixes.B.map(e =>
    new MatrixElementWritable(
      new Text(e.row.toString),
      new Text(e.col.toString),
      new DoubleWritable(e.value),
      new BooleanWritable(false)
    )
  )
  
  val output = SampleMatrixes.C.map(e => new Text(e.row + "\t" + e.col + "\t" + e.value))
  
  
  "Two-phase MRMultiply" should "output correct matrix" in {
    val mapper1 = new SubmatrixMultiplyMapper()
    val reducer1 = new SubmatrixMultiplyReducer()
    val mr1Driver = MapReduceDriver.newMapReduceDriver(mapper1, reducer1)
    mr1Driver.getConfiguration().setInt("N_SUBMATRIX_DIVS", 2)
    for (element <- input) {
      mr1Driver.withInput(NullWritable.get, element)
    }
    val phase1Result = mr1Driver.run()
    
    val mapper2 = new MatrixAggregateMapper()
    val reducer2 = new MatrixAggregateReducer()
    val combiner2 = new MatrixAggregateCombiner()
    val mr2Driver = MapReduceDriver.newMapReduceDriver(mapper2, reducer2, combiner2)
    for (pair <- phase1Result) {
      val Array(r, c, v) = pair.getFirst().toString().split("\t")
      val element = new MatrixElementWritable(new Text(r), new Text(c), new DoubleWritable(v.toDouble))
      mr2Driver.withInput(NullWritable.get, element)
    }
    
    val phase2Result = mr2Driver.run().map(pair => pair.getFirst().toString())
    
    val expected = SampleMatrixes.C.map(element => element.row + "\t" + element.col + "\t" + element.value)
    
    phase2Result should contain theSameElementsAs (expected)
  }
  
  "Single-phase MRMultiply" should "output correct matrix" in {
    val mapper = new SubmatrixMultiplyMapper()
    val reducer = new SubmatrixMultiplyReducer()
    val mrDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer)
    mrDriver.getConfiguration().setInt("N_SUBMATRIX_DIVS", 2)
    mrDriver.getConfiguration().setBoolean(MRMultiply.ONE_MR_CONF_FLAG, true)
    for (element <- input) {
      mrDriver.withInput(NullWritable.get, element)
    }
    val result = mrDriver.run().map(pair => pair.getFirst().toString())
    
    val expected = SampleMatrixes.C.map(element => element.row + "\t" + element.col + "\t" + element.value)
    
    result should contain theSameElementsAs (expected)
  }
  
  "Partitioner" should "group output submatrix components together" in {
    val partitioner = new SubmatrixPartitioner()
    val conf = new Configuration()
    val divs = 4
    val tasks = divs * divs
    conf.setInt("N_SUBMATRIX_DIVS", divs)
    partitioner.setConf(conf)
    
    for (x <- 0 until divs; z <- 0 until divs) {
      def key(y: Int) = new IntTripleWritable(new IntWritable(x), new IntWritable(y), new IntWritable(z))
      val expectation = partitioner.getPartition(key(0), null, tasks)
      for (y <- 1 until divs) {
        partitioner.getPartition(key(y), null, tasks) should be (expectation)
      }
    }
  }
  
  
}
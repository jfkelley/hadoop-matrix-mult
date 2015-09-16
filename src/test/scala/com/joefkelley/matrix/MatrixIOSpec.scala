package com.joefkelley.matrix

import org.scalatest._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.conf.Configuration
import com.joefkelley.matrix.mr.LeftMatrixInputFormat
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.mapreduce.RecordReader
import com.joefkelley.matrix.mr.LineToMatrixElement
import com.joefkelley.matrix.mr.MatrixAggregateCombiner
import com.joefkelley.matrix.mr.MatrixAggregateMapper
import com.joefkelley.matrix.mr.MatrixAggregateReducer
import com.joefkelley.matrix.mr.SubmatrixMultiplyReducer
import com.joefkelley.matrix.mr.SubmatrixMultiplyMapper
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver
import com.joefkelley.matrix.mr.MatrixElementWritable
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BooleanWritable
import scala.collection.JavaConversions._
import com.joefkelley.matrix.mr.MatrixElementToLine

@RunWith(classOf[JUnitRunner])
class MatrixIOSpec extends FlatSpec with Matchers with BeforeAndAfter {
  
  var csvInput: File = _
  var tsvInput: File = _
  
  before {
    csvInput = createInputFile(',', "csv")
    tsvInput = createInputFile('\t', "tsv")
  }
  
  def createInputFile(delim: Character, suffix: String): File = {
    val f = File.createTempFile("mr-multiply-test-input", suffix)
    val wr = new PrintWriter(new FileWriter(f))
    try {
      wr.println(s"0${delim}0${delim}1.0")
      wr.println(s"0${delim}1${delim}2.0")
    } finally {
      wr.close()
    }
    f
  }
  
  after {
    csvInput.delete()
    tsvInput.delete()
  }
  
  "default matrix input format" should "read correct values" in {
    val conf = new Configuration(false)
    conf.set("fs.default.name", "file:///")

    val path = new Path(tsvInput.getAbsoluteFile().toURI())
    val split = new FileSplit(path, 0, tsvInput.length(), null)

    val inputFormat = ReflectionUtils.newInstance(classOf[LeftMatrixInputFormat], conf)
    val context = new TaskAttemptContextImpl(conf, new TaskAttemptID())
    val reader = inputFormat.createRecordReader(split, context)

    reader.initialize(split, context)
    
    reader.nextKeyValue() should be (true)
    reader.getCurrentValue().row.toString() should be ("0")
    reader.getCurrentValue().col.toString() should be ("0")
    reader.getCurrentValue().value.get() should be (1.0)
    reader.nextKeyValue() should be (true)
    reader.getCurrentValue().row.toString() should be ("0")
    reader.getCurrentValue().col.toString() should be ("1")
    reader.getCurrentValue().value.get() should be (2.0)
    reader.nextKeyValue() should be (false)

  }
  
  "csv matrix input format" should "read correct values" in {
    val conf = new Configuration(false)
    conf.setClass(MRMultiply.INPUT_CONVERTER_KEY, classOf[CSVLineToMatrixElement], classOf[LineToMatrixElement])
    conf.set("fs.default.name", "file:///")

    val path = new Path(csvInput.getAbsoluteFile().toURI())
    val split = new FileSplit(path, 0, csvInput.length(), null)

    val inputFormat = ReflectionUtils.newInstance(classOf[LeftMatrixInputFormat], conf)
    val context = new TaskAttemptContextImpl(conf, new TaskAttemptID())
    val reader = inputFormat.createRecordReader(split, context)

    reader.initialize(split, context)
    
    reader.nextKeyValue() should be (true)
    reader.getCurrentValue().row.toString() should be ("0")
    reader.getCurrentValue().col.toString() should be ("0")
    reader.getCurrentValue().value.get() should be (1.0)
    reader.nextKeyValue() should be (true)
    reader.getCurrentValue().row.toString() should be ("0")
    reader.getCurrentValue().col.toString() should be ("1")
    reader.getCurrentValue().value.get() should be (2.0)
    reader.nextKeyValue() should be (false)

  }
  
  
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
  
  "Two-phase MRMultiply with CSV IO" should "output correct matrix" in {
    val mapper1 = new SubmatrixMultiplyMapper()
    val reducer1 = new SubmatrixMultiplyReducer()
    val mr1Driver = MapReduceDriver.newMapReduceDriver(mapper1, reducer1)
    mr1Driver.getConfiguration().setInt("N_SUBMATRIX_DIVS", 2)
    mr1Driver.getConfiguration().setClass(MRMultiply.INPUT_CONVERTER_KEY, classOf[CSVLineToMatrixElement], classOf[LineToMatrixElement])
    mr1Driver.getConfiguration().setClass(MRMultiply.OUTPUT_CONVERTER_KEY, classOf[CSVMatrixElementToLine], classOf[MatrixElementToLine])
    for (element <- input) {
      mr1Driver.withInput(NullWritable.get, element)
    }
    val phase1Result = mr1Driver.run()
    
    val mapper2 = new MatrixAggregateMapper()
    val reducer2 = new MatrixAggregateReducer()
    val combiner2 = new MatrixAggregateCombiner()
    val mr2Driver = MapReduceDriver.newMapReduceDriver(mapper2, reducer2, combiner2)
    mr2Driver.getConfiguration().setClass(MRMultiply.INPUT_CONVERTER_KEY, classOf[CSVLineToMatrixElement], classOf[LineToMatrixElement])
    mr2Driver.getConfiguration().setClass(MRMultiply.OUTPUT_CONVERTER_KEY, classOf[CSVMatrixElementToLine], classOf[MatrixElementToLine])
    for (pair <- phase1Result) {
      val Array(r, c, v) = pair.getFirst().toString().split(",")
      val element = new MatrixElementWritable(new Text(r), new Text(c), new DoubleWritable(v.toDouble))
      mr2Driver.withInput(NullWritable.get, element)
    }
    
    val phase2Result = mr2Driver.run().map(pair => pair.getFirst().toString())
    
    val expected = SampleMatrixes.C.map(element => element.row + "," + element.col + "," + element.value)
    
    phase2Result should contain theSameElementsAs (expected)
  }
  
}
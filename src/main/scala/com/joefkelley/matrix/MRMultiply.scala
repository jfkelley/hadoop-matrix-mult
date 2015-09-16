package com.joefkelley.matrix

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import com.joefkelley.matrix.mr.IntTripleWritable
import com.joefkelley.matrix.mr.LeftMatrixInputFormat
import com.joefkelley.matrix.mr.MatrixAggregateCombiner
import com.joefkelley.matrix.mr.MatrixAggregateMapper
import com.joefkelley.matrix.mr.MatrixAggregateReducer
import com.joefkelley.matrix.mr.MatrixElementWritable
import com.joefkelley.matrix.mr.RightMatrixInputFormat
import com.joefkelley.matrix.mr.SubmatrixMultiplyMapper
import com.joefkelley.matrix.mr.SubmatrixMultiplyReducer
import com.joefkelley.matrix.mr.SubmatrixPartitioner
import org.apache.log4j.Logger
import java.util.UUID

object MRMultiply extends Configured with Tool {
  
  final val ONE_MR_CONF_FLAG = "matrix.multiply.one.mr"
  final val INPUT_CONVERTER_KEY = "matrix.multiply.input.converter"
  final val OUTPUT_CONVERTER_KEY = "matrix.multiply.output.converter"
  
  private final val Log: Logger = Logger.getLogger(this.getClass)
  
  override def run(args: Array[String]): Int = {
    def parse(args: List[String], soFar: Map[String, String]): Map[String, String] = args match {
      case Nil => soFar
      case "--left"            :: left       :: rest => parse(rest, soFar + ("left" ->       left))
      case "--right"           :: right      :: rest => parse(rest, soFar + ("right" ->      right))
      case "--output"          :: output     :: rest => parse(rest, soFar + ("output" ->     output))
      case "--nDivs"           :: nDivs      :: rest => parse(rest, soFar + ("nDivs" ->      nDivs))
      case "--oneMR"                         :: rest => parse(rest, soFar + ("oneMR" ->      "true"))
      case "--inputConverter"  :: inputConv  :: rest => parse(rest, soFar + ("inputConv" ->  inputConv))
      case "--outputConverter" :: outputConv :: rest => parse(rest, soFar + ("outputConv" -> outputConv))
      case _ :: rest => parse(rest, soFar)
    }
    val parsed = parse(args.toList, Map.empty)
    
    val option = for (
        left <- parsed.get("left");
        right <- parsed.get("right");
        output <- parsed.get("output");
        nDivs <- getNDivs(parsed)
      ) yield (left, right, output, nDivs)
      
    option match {
      case Some((left, right, output, nDivs)) =>
        run(
            left,
            right,
            output,
            nDivs.toInt,
            parsed.getOrElse("oneMR", "false").toBoolean,
            parsed.get("inputConv"),
            parsed.get("outputConv")
            )
      case None => throw new IllegalArgumentException("Expected arguments: --left [path] --right [path] --output [path] --nDivs [int] OR --oneMR")
    }
  }
  
  def getNDivs(parsed: Map[String, String]): Option[Int] = {
    if (parsed.getOrElse("oneMR", "false").toBoolean) {
      val nReduceTasks = getConf().getInt("mapred.reduce.tasks", 1)
      val nDivs = Math.round(Math.sqrt(nReduceTasks)).toInt
      if (nDivs * nDivs != nReduceTasks) {
        Log.warn(s"Number of reduce tasks = $nReduceTasks is not a square number and so cannot be used with --oneMR. Setting number of reduce tasks to ${nDivs * nDivs}")
        getConf().setInt("mapred.reduce.tasks", nDivs * nDivs)
      }
      Some(nDivs)
    } else {
      parsed.get("nDivs").map(_.toInt)
    }
  }
  
  def run(leftStr: String, rightStr: String, outputStr: String, nDivs: Int, oneMR: Boolean, inputConv: Option[String], outputConv: Option[String]): Int = {
    if (oneMR) {
      getConf().setBoolean(ONE_MR_CONF_FLAG, true)
    }
    for (inConv <- inputConv; outConv <- outputConv) {
      getConf().set(INPUT_CONVERTER_KEY, inConv)
      getConf().set(OUTPUT_CONVERTER_KEY, outConv)
    }
    val (left, right, output) = (new Path(leftStr), new Path(rightStr), new Path(outputStr))
    val temp = new Path(getConf().get("hadoop.tmp.dir"), "mr_multiply_intermediate_" + UUID.randomUUID().toString())
    try {
      val result1 = runJob1(left, right, if (oneMR) output else temp, nDivs)
      if (!oneMR && result1 == 0) {
        runJob2(temp, output)
      } else {
        result1
      }
    } finally {
      val fs = temp.getFileSystem(getConf)
      fs.delete(temp, true)
    }
  }
  
  def runJob1(left: Path, right: Path, output: Path, nDivs: Int): Int = {
    val job = Job.getInstance(getConf())
    
    job.getConfiguration().setInt("N_SUBMATRIX_DIVS", nDivs)
    
    job.setJobName("Matrix Multiply (phase 1/2)")
    job.setJarByClass(this.getClass())
    MultipleInputs.addInputPath(job, left, classOf[LeftMatrixInputFormat])
    MultipleInputs.addInputPath(job, right, classOf[RightMatrixInputFormat])
    job.setMapperClass(classOf[SubmatrixMultiplyMapper])
    job.setMapOutputKeyClass(classOf[IntTripleWritable])
    job.setMapOutputValueClass(classOf[MatrixElementWritable])
    job.setPartitionerClass(classOf[SubmatrixPartitioner])
    job.setReducerClass(classOf[SubmatrixMultiplyReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[NullWritable])
    FileOutputFormat.setOutputPath(job, output)
    
    val result = job.waitForCompletion(false)
    if (result) 0 else 1
  }
  
  def runJob2(input: Path, output: Path): Int = {
    val job = Job.getInstance(getConf())
    
    job.setJobName("Matrix Multiply (phase 2/2)")
    job.setJarByClass(this.getClass())
    job.setInputFormatClass(classOf[LeftMatrixInputFormat])
    FileInputFormat.addInputPath(job, input)
    job.setMapperClass(classOf[MatrixAggregateMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[DoubleWritable])
    job.setCombinerClass(classOf[MatrixAggregateCombiner])
    job.setReducerClass(classOf[MatrixAggregateReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[NullWritable])
    FileOutputFormat.setOutputPath(job, output)
    
    val result = job.waitForCompletion(false)
    if (result) 0 else 1
  }
  
  def main(args: Array[String]): Unit = {
    val result = ToolRunner.run(this, args)
    System.exit(result)
  }
  
}
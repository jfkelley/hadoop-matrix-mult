package com.joefkelley.matrix

import org.scalatest._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MatrixSpec extends FlatSpec with Matchers {
  
  "empty matrix builder" should "have zero rows/cols" in {
    new MatrixBuilder().nRows() should be (0)
    new MatrixBuilder().nCols() should be (0)
  }
  
  "matrix builder" should "have correct dimensions" in {
    val mb = new MatrixBuilder(Seq(new MatrixElement(2, 3, 1.0)))
    mb.nRows() should be (3)
    mb.nCols() should be (4)
  }
  
  "empty matrix builder" should "generate empty CRS" in {
    val crs = new MatrixBuilder().buildCompressedRowStorage()
    crs.colIndexes should be (Array.empty[Int])
    crs.data should be (Array.empty[Double])
    crs.nCols should be (0)
    crs.nRows should be (0)
    crs.rowPointers should be (Array(0))
  }
  
  "empty matrix builder" should "generate empty CCS" in {
    val ccs = new MatrixBuilder().buildCompressedColStorage()
    ccs.rowIndexes should be (Array.empty[Int])
    ccs.data should be (Array.empty[Double])
    ccs.nCols should be (0)
    ccs.nRows should be (0)
    ccs.colPointers should be (Array(0))
  }
  
  "matrix builder" should "generate correct CRS" in {
    val crs = new MatrixBuilder(SampleMatrixes.A).buildCompressedRowStorage()
    crs.nRows should be (3)
    crs.nCols should be (4)
    crs.data should be (Array(1, 2, 3, -2, 2, 1, 1, 2))
    crs.colIndexes should be (Array(0, 1, 3, 0, 2, 1, 2, 3))
    crs.rowPointers should be (Array(0, 3, 5, 8))
  }
  
  "matrix builder" should "generate correct CCS" in {
    val ccs = new MatrixBuilder(SampleMatrixes.A).buildCompressedColStorage()
    ccs.nRows should be (3)
    ccs.nCols should be (4)
    ccs.data should be (Array(1, -2, 2, 1, 2, 1, 3, 2))
    ccs.rowIndexes should be (Array(0, 1, 0, 2, 1, 2, 0, 2))
    ccs.colPointers should be (Array(0, 2, 4, 6, 8))
  }
  
  "empty matrix multiplication" should "output empty matrix" in {
    val crs = new MatrixBuilder().buildCompressedRowStorage()
    val ccs = new MatrixBuilder().buildCompressedColStorage()
    val result = CompressedMatrixMultiply.multiply(crs, ccs)
    result shouldBe empty
  }
  
  "matrix multiplication" should "output correct matrix" in {
    val crs = new MatrixBuilder(SampleMatrixes.A).buildCompressedRowStorage()
    val ccs = new MatrixBuilder(SampleMatrixes.B).buildCompressedColStorage()
    val result = CompressedMatrixMultiply.multiply(crs, ccs)
    result should contain theSameElementsAs SampleMatrixes.C
  }
  
}
package com.joefkelley.matrix

  /*
   * These matrixes are used tests:
   * 
   *     A    *    B    =    C
   * 
   *  1 2 0 3   0 0 1 0   8 0 7 0
   * -2 0 2 0 * 1 0 3 0 = 0 0 0 4
   *  0 1 1 2   0 0 1 2   5 0 4 2
   *  0 0 0 0   2 0 0 0   0 0 0 0
   */
object SampleMatrixes {
  
  val A = Seq(
      new MatrixElement(0, 0, 1.0),
      new MatrixElement(0, 1, 2.0),
      new MatrixElement(0, 3, 3.0),
      new MatrixElement(1, 0, -2.0),
      new MatrixElement(1, 2, 2.0),
      new MatrixElement(2, 1, 1.0),
      new MatrixElement(2, 2, 1.0),
      new MatrixElement(2, 3, 2.0)
      )
      
  val B = Seq(
      new MatrixElement(0, 2, 1.0),
      new MatrixElement(1, 0, 1.0),
      new MatrixElement(1, 2, 3.0),
      new MatrixElement(2, 2, 1.0),
      new MatrixElement(2, 3, 2.0),
      new MatrixElement(3, 0, 2.0)
      )
  
  val C = Seq(
      new MatrixElement(0, 0, 8.0),
      new MatrixElement(0, 2, 7.0),
      new MatrixElement(1, 3, 4.0),
      new MatrixElement(2, 0, 5.0),
      new MatrixElement(2, 2, 4.0),
      new MatrixElement(2, 3, 2.0)
      )
}
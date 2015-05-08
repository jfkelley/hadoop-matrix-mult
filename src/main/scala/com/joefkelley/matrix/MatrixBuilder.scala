package com.joefkelley.matrix

class MatrixBuilder(elements: Seq[MatrixElement] = Nil) {
  
  def +(x: MatrixElement) = new MatrixBuilder(x +: elements)
  def ++(xs: Seq[MatrixElement]) = new MatrixBuilder(xs ++: elements)
  
  def buildCompressedRowStorage(): CompressedRowStorage = {
    val arr = elements.sortBy{ m => (m.row, m.col) }.toArray
    new CompressedRowStorage(nRows, nCols, arr)
  }
  
  def buildCompressedColStorage(): CompressedColStorage = {
    val arr = elements.sortBy{ m => (m.col, m.row) }.toArray
    new CompressedColStorage(nRows, nCols, arr)
  }
  
  def nRows(): Int = if (elements.isEmpty) 0 else elements.map(_.row).max + 1
  def nCols(): Int = if (elements.isEmpty) 0 else elements.map(_.col).max + 1
  
}
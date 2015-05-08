package com.joefkelley.matrix;

public class CompressedColStorage {

	public final int nRows;
	public final int nCols;
	public final int[] colPointers;
	public final int[] rowIndexes;
	public final double[] data;
	
	public CompressedColStorage(int nRows, int nCols, MatrixElement[] elems) {
		this.nRows = nRows;
		this.nCols = nCols;
		colPointers = new int[nCols + 1];
		rowIndexes = new int[elems.length];
		data = new double[elems.length];
		MatrixElement prev = null;
		int curCol = 0;
		for (int i = 0; i < elems.length; i++) {
			MatrixElement e = elems[i];
			if (prev != null) {
				if (prev.col > e.col || (prev.col == e.col && prev.row >= e.row)) {
					throw new IllegalArgumentException("Matrix elements are not in order (sorted by column, then row)");
				}
			}
			while (e.col != curCol) {
				curCol++;
				colPointers[curCol] = i;
			}
			rowIndexes[i] = e.row;
			data[i] = e.value;
			prev = e;
		}
		curCol++;
		while (curCol <= nCols) {
			colPointers[curCol] = elems.length;
			curCol++;
		}
	}
}

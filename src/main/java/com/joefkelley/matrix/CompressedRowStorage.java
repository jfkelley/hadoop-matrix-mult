package com.joefkelley.matrix;


public class CompressedRowStorage {

	public final int nRows;
	public final int nCols;
	public final int[] rowPointers;
	public final int[] colIndexes;
	public final double[] data;
	
	public CompressedRowStorage(int nRows, int nCols, MatrixElement[] elems) {
		this.nRows = nRows;
		this.nCols = nCols;
		rowPointers = new int[nRows + 1];
		colIndexes = new int[elems.length];
		data = new double[elems.length];
		MatrixElement prev = null;
		int curRow = 0;
		for (int i = 0; i < elems.length; i++) {
			MatrixElement e = elems[i];
			if (prev != null) {
				if (prev.row > e.row || (prev.row == e.row && prev.col >= e.col)) {
					throw new IllegalArgumentException("Matrix elements are not in order (sorted by row, then column)");
				}
			}
			while (e.row != curRow) {
				curRow++;
				rowPointers[curRow] = i;
			}
			colIndexes[i] = e.col;
			data[i] = e.value;
			prev = e;
		}
		curRow++;
		while (curRow <= nRows) {
			rowPointers[curRow] = elems.length;
			curRow++;
		}
	}
	
}

package com.joefkelley.matrix;

import java.util.ArrayList;
import java.util.List;

public class CompressedMatrixMultiply {
	
	public static MatrixElement[] multiply(CompressedRowStorage a, CompressedColStorage b) {
		List<int[]> outputRows = new ArrayList<int[]>();
		List<int[]> outputCols = new ArrayList<int[]>();
		List<double[]> outputVals = new ArrayList<double[]>();
		int chunkSize = 1024 * 128;
		int[] curOutputRows = new int[chunkSize];
		int[] curOutputCols = new int[chunkSize];
		double[] curOutputVals = new double[chunkSize];
		int nRows = a.nRows;
		int nCols = b.nCols;
		int[] rowPointers = a.rowPointers;
		int[] colPointers = b.colPointers;
		int[] colIndexes = a.colIndexes;
		int[] rowIndexes = b.rowIndexes;
		double[] aData = a.data;
		double[] bData = b.data;
		int i = 0;
		for (int r = 0; r < nRows; r++) {
			for (int c = 0; c < nCols; c++) {
				double sum = 0.0;
				int i1 = rowPointers[r];
				int i2 = colPointers[c];
				int end1 = rowPointers[r+1];
				int end2 = colPointers[c+1];
				while (i1 < end1 && i2 < end2) {
					if (colIndexes[i1] == rowIndexes[i2]) {
						sum += aData[i1++] * bData[i2++];
					} else if (colIndexes[i1] < rowIndexes[i2]) {
						i1++;
					} else {
						i2++;
					}
				}
				if (sum != 0.0) {
					curOutputRows[i] = r;
					curOutputCols[i] = c;
					curOutputVals[i] = sum;
					i++;
					if (i == chunkSize) {
						outputRows.add(curOutputRows);
						outputCols.add(curOutputCols);
						outputVals.add(curOutputVals);
						curOutputRows = new int[chunkSize];
						curOutputCols = new int[chunkSize];
						curOutputVals = new double[chunkSize];
						i = 0;
					}
				}
			}
		}
		
		MatrixElement[] result = new MatrixElement[outputRows.size() * chunkSize + i];
		int outI = 0;
		for (int j = 0; j < outputRows.size(); j++) {
			for (int k = 0; k < chunkSize; k++) {
				int row = outputRows.get(j)[k];
				int col = outputCols.get(j)[k];
				double val = outputVals.get(j)[k];
				result[outI++] = new MatrixElement(row, col, val);
			}
		}
		for (int k = 0; k < i; k++) {
			int row = curOutputRows[k];
			int col = curOutputCols[k];
			double val = curOutputVals[k];
			result[outI++] = new MatrixElement(row, col, val);
		}
		
		return result;
	}
	
}

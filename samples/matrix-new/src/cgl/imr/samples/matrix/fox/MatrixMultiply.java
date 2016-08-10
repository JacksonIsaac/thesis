/*
 * Software License, Version 1.0
 *
 *  Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 *
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1) All redistributions of source code must retain the above copyright notice,
 *  the list of authors in the original source code, this list of conditions and
 *  the disclaimer listed in this license;
 * 2) All redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the disclaimer listed in this license in
 *  the documentation and/or other materials provided with the distribution;
 * 3) Any documentation included with all redistributions must include the
 *  following acknowledgement:
 *
 * "This product includes software developed by the Community Grids Lab. For
 *  further information contact the Community Grids Lab at
 *  http://communitygrids.iu.edu/."
 *
 *  Alternatively, this acknowledgement may appear in the software itself, and
 *  wherever such third-party acknowledgments normally appear.
 *
 * 4) The name Indiana University or Community Grids Lab or Twister,
 *  shall not be used to endorse or promote products derived from this software
 *  without prior written permission from Indiana University.  For written
 *  permission, please contact the Advanced Research and Technology Institute
 *  ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 * 5) Products derived from this software may not be called Twister,
 *  nor may Indiana University or Community Grids Lab or Twister appear
 *  in their name, without prior written permission of ARTI.
 *
 *
 *  Indiana University provides no reassurances that the source code provided
 *  does not infringe the patent or any other intellectual property rights of
 *  any other entity.  Indiana University disclaims any liability to any
 *  recipient for claims brought by any other entity based on infringement of
 *  intellectual property rights or otherwise.
 *
 * LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO
 * WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 * NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF
 * INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS.
 * INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS",
 * "VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.
 * LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR
 * ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION
 * GENERATED USING SOFTWARE.
 */

package cgl.imr.samples.matrix.fox;

import java.io.IOException;
import java.util.Iterator;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.Key;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterModel;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.impl.GenericCombiner;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.types.IntValue;

/**
 * MapReduce program performing Matrix multiplication. The algorithm used is as
 * follows.
 * 
 * Let the matrices be A x B = C
 * 
 * Main Program 1. Partition matrix B in to column blocks (number of column
 * blocks = number of map tasks) 2. Partition matrix A in to row blocks (number
 * of row blocks = number of iterations, typically should be decided by the size
 * of the memory requirements) 3. Configure map tasks with the column blocks of
 * B 4. foreach row block 5. Run MapReduce by sending a row block to all the map
 * tasks. (In iteration i send the ith row block) 6. Append the resulting row
 * block to the output matrix C. 7. end for
 * 
 * Map Task 1. Multiply the assigned column block with the current row block. 2.
 * Collect the resulting block of the output matrix.
 * 
 * Reduce Task 1. Collect all the matrix blocks for and put them in their
 * correct order to form a row block of the final output matrix. 2. Collect this
 * row block.
 * 
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * 
 */
public class MatrixMultiply {

	private static UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	private static void appendRowBlockToMatrix(double[][] data,
			MatrixData rowBlock, int start) {
		int width = rowBlock.getWidth();
		int end = rowBlock.getHeight() + start;
		double[][] rowData = rowBlock.getData();
		int count = 0;
		for (int i = start; i < end; i++) {
			for (int j = 0; j < width; j++) {
				data[i][j] = rowData[count][j];
			}
			count++;
		}
	}

	public static MatrixData[] splitMatrixIntoBlocks(double[][] input, int n,
			int matNo) {

		MatrixData[] output = new MatrixData[n * n];
		MatrixData mBlock = null;
		int len = input.length;
		int d = len / n;
		int xStart = 0;
		int xEnd = 0;
		double block[][] = null;

		int yStart = 0;
		int yEnd = 0;

		int xCount = 0;
		int yCount = 0;

		for (int i = 0; i < n; i++) {
			xEnd += d;
			yStart = 0;
			yEnd = 0;
			for (int j = 0; j < n; j++) {
				yEnd += d;
				block = new double[d][d];
				xCount = 0;
				for (int k = xStart; k < xEnd; k++) {
					yCount = 0;
					for (int m = yStart; m < yEnd; m++) {
						block[xCount][yCount] = input[k][m];
						yCount++;
					}
					xCount++;
				}
				mBlock = new MatrixData(block, d, d, i, j, matNo);
				output[i * n + j] = mBlock;
				yStart = yEnd;
			}
			xStart = xEnd;
		}

		return output;

	}

	public static void testBlockDecompostion() {
		int dim = 16;
		int n = 4;
		int d = 4;
		double[][] data = new double[dim][dim];
		for (int i = 0; i < dim; i++) {
			for (int j = 0; j < dim; j++) {
				data[i][j] = i + j;
				System.out.print(data[i][j] + " ");
			}
			System.out.println();
		}

		MatrixData[] blocks = splitMatrixIntoBlocks(data, n, 0);
		for (MatrixData m : blocks) {
			System.out.println(m.getRow() + "  " + m.getCol());
			data = m.getData();
			d = m.getHeight();
			for (int i = 0; i < d; i++) {
				for (int j = 0; j < d; j++) {
					System.out.print(data[i][j] + " ");
				}
				System.out.println();
			}
			System.out.println();
			System.out.println();
		}
	}

	public static void main(String[] args) {

		// /testBlockDecompostion();
		// System.exit(0);

		String module = "ParallelMatMult.main() ->";
		if (args.length != 5) {
			String errorReport = module
					+ "The Correct arguments for the square matrix multiplication \n"
					+ "[data file A] - binary data file - should be read as Double \n"
					+ "[data file B] - binary data file - should be read as Double \n"
					+ "[output file] - this is the output file] \n"
					+ "[n] - the dimension of the procesor matrix] \n"
					+ "[block size] - this is the block size for the block matrix multiplication. \n"
					+ "   This is different to the initial data breakup using the number of map tasks. \n"
					+ "   This is a more fine grain block value probably in the range of 64 - 128 and \n"
					+ "   will be usefull for the cache optimization> \n";
			System.out.println(errorReport);
			System.exit(0);
		}
		String dataFileA = args[0];
		String dataFileB = args[1];
		String outputFile = args[2];
		int n = Integer.parseInt(args[3]);
		int bz = Integer.parseInt(args[4]);
		try {
			matrixMultiplyMapReduce(dataFileA, dataFileB, outputFile, n, bz);

		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		System.exit(0);
	}

	/**
	 * Perform Matrix multiplication operation using MapReduce technique.
	 * 
	 * @param matAFileName
	 *            - File name of Matrix A.
	 * @param matBFileName
	 *            - File name of Matrix B.
	 * @param outFileName
	 *            - File name to store the ouput matrix.
	 * @param numMaps
	 *            - Number of map tasks.
	 * @param numIterations
	 *            - Number of iterations to use.
	 * @param blockSize
	 *            - Block size to do block decomposition - A mechanism to
	 *            enhance the cache performance, not the parallelism.
	 * @throws IOException
	 */
	public static void matrixMultiplyMapReduce(String matAFileName,
			String matBFileName, String outFileName, int n, int blockSize)
			throws TwisterException, IOException {

		// Dimension of the process space.
		int nSquare = n * n;

		MatrixData matA = new MatrixData();
		MatrixData matB = new MatrixData();

		matA.loadDataFromBinFile(matAFileName);
		matB.loadDataFromBinFile(matBFileName);

		if (!((matA.getHeight() == matA.getWidth())
				&& (matB.getHeight() == matB.getWidth())
				&& (matA.getHeight() == matB.getHeight()) && (matA.getHeight()
				% n == 0))) {
			throw new TwisterException(
					"This version of matrix multiplication assums square matrices and the number of map tasks should evenly divide the dimension of a matrix. This is not an absolute constraint for this algorithm. It is just for easiness in implementation.");
		}

		double beginTime = System.currentTimeMillis();

		int dim = matB.getWidth();
		int d = matA.getHeight() / n;

		// System.out.println("Time to load data ="+(midTime-beginTime)/1000);

		// JobConfigurations
		JobConf jobConf = new JobConf("fully-in-mem-mat-mult"
				+ uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(MatrixMultiplyMapTask.class);
		jobConf.setReducerClass(MatrixMultiplyReduceTask.class);
		jobConf.setCombinerClass(GenericCombiner.class);
		jobConf.setNumMapTasks(nSquare);
		jobConf.setNumReduceTasks(nSquare);
		jobConf.addProperty("n", String.valueOf(n));
		jobConf.addProperty("d", String.valueOf(d));
		jobConf.addProperty("block_size", String.valueOf(blockSize));
		jobConf.addProperty("final_width", String.valueOf(dim));
		jobConf.setReducerSelectorClass(IntNoBasedReducerSelector.class);
		jobConf.setRowBCastSupported(true);
		//jobConf.setFaultTolerance();

		MatrixData[] A_Blocks = splitMatrixIntoBlocks(matA.getData(), n, 0);
		matA = null; // We don't need it anymore.

		// Split matB in to Square blocks.
		MatrixData[] B_Blocks = splitMatrixIntoBlocks(matB.getData(), n, 1);
		matB = null; // We don't need it anymore.
		System.gc();

		// Create an array of A and B blocks as the input to map tasks.
		MapData[] mapData = new MapData[A_Blocks.length];
		for (int i = 0; i < A_Blocks.length; i++) {
			mapData[i] = new MapData(A_Blocks[i], B_Blocks[i]);
		}

		double endTime = System.currentTimeMillis();
		System.out.println("Time to split data =" + (endTime - beginTime)
				/ 1000);
		double midTime = System.currentTimeMillis();

		double[][] outMat = null;
		MatrixData outBlock;

		TwisterModel driver = null;
		TwisterMonitor monitor = null;
		GenericCombiner combiner;
		try {
			driver = new TwisterDriver(jobConf);
			driver.configureMaps(mapData);
			endTime = System.currentTimeMillis();
			System.out.println("Time to configure maps =" + (endTime - midTime)
					/ 1000);
			midTime = System.currentTimeMillis();

			for (int i = 0; i < n; i++) {
				monitor = driver.runMapReduceBCast(new IntValue(i));
				monitor.monitorTillCompletion();
				combiner = (GenericCombiner) driver.getCurrentCombiner();

				if (i == (n - 1)) {
					outMat = new double[dim][dim];
					System.out.println("Merging.......");
					if (!combiner.getResults().isEmpty()) {
						Iterator<Key> ite = combiner.getResults().keySet()
								.iterator();
						while (ite.hasNext()) {
							outBlock = (MatrixData) combiner.getResults().get(
									ite.next());
							//appendABlockToMatrix(outBlock, outMat, d);
						}
					}
				}

				System.out.println("Iteration No: " + i);
				// System.exit(-1);
			}
			endTime = System.currentTimeMillis();
			System.out.println("Mat mult time =" + (endTime - midTime) / 1000);

		} catch (TwisterException e) {
			driver.close();
			throw e;
		}

		MatrixData outMatrix = new MatrixData(outMat, dim, dim);
		//outMatrix.writeToBinFile(outFileName);
		System.out
				.println("------------------------------------------------------");
		System.out
				.println("Total matrix multiplication (except data loading and final writing )took "
						+ (endTime - beginTime) / 1000 + " seconds.");
		System.out
				.println("------------------------------------------------------");

		// Print 3x3 block. Just for clarification.
		printFirstNRowsOfMatrix(outMat, 3, dim);

		/*
		 * MatrixData mA = new MatrixData(); MatrixData mB = new MatrixData();
		 * mA.loadDataFromBinFile(matAFileName);
		 * mB.loadDataFromBinFile(matBFileName);
		 * if(verify(mA.getData(),mB.getData(),outMat,4)){
		 * System.out.println("VERIFIED!"); }
		 */

		driver.close();
	}

	private static MatrixData[] getBlocksOfAMatrix(MatrixData mat, int n,
			int d, int matNo) {

		MatrixData[] rowBlocks = splitMatrixRowWise(mat, n, matNo);
		MatrixData[] output = new MatrixData[n * n];

		int count = 0;
		for (MatrixData rowB : rowBlocks) {
			double[][] rectBlock = rowB.getData();
			double[][] squrBlock;
			int row = rowB.getRow();
			int col = rowB.getCol();

			int begin = 0;
			int end = 0;

			for (int i = 0; i < n; i++) {
				squrBlock = new double[d][d];
				end += d;
				for (int j = 0; j < d; j++) {
					for (int k = begin; k < end; k++) {
						squrBlock[j][k - begin] = rectBlock[j][k];
					}
				}
				begin = end;
				output[count] = new MatrixData(squrBlock, d, d, row, col, matNo);
				count++;
			}
		}
		return output;
	}

	private static void appendABlockToMatrix(MatrixData outBlock,
			double[][] outMat, int d) {
		int col = outBlock.getCol();
		int row = outBlock.getRow();
		int xStart = row * d;
		int xEnd = row * d + d;
		int yStart = col * d;
		int yEnd = col * d + d;
		double[][] data = outBlock.getData();

		for (int i = xStart; i < xEnd; i++) {
			for (int j = yStart; j < yEnd; j++) {
				outMat[i][j] = data[i - xStart][j - yStart];
			}
		}

	}

	private static void printFirstNRowsOfMatrix(double[][] data, int n,
			int width) {
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < n; j++) {
				System.out.print(data[i][j] + " ");
			}
			System.out.println();
		}
	}

	/**
	 * Splits a given matrix into a set of column blocks.
	 * 
	 * @param mat
	 * @param numMaps
	 * @return
	 */
	private static MatrixData[] splitMatrixColumnWise(MatrixData mat,
			int numMaps, int matNo) {
		int width = mat.getWidth();
		int colWidth = width / numMaps;
		int rem = width % numMaps;

		MatrixData[] columns = new MatrixData[numMaps];
		double[][] data = mat.getData();
		double[][] column;
		int start = 0;
		int end = 0;
		int curWidth = 0;
		int count = 0;
		for (int i = 0; i < numMaps; i++) {
			end += colWidth;
			if (rem > 0) {
				end++;
				rem--;
			}
			curWidth = end - start;
			column = new double[mat.getHeight()][curWidth];
			count = 0;
			for (int j = start; j < end; j++) {
				column[i][count] = data[i][j];
				count++;

			}
			columns[i] = new MatrixData(data, mat.getHeight(), curWidth, matNo);
			columns[i].setCol(i);
			start = end;
		}
		return columns;
	}

	/**
	 * Splits a given matrix into a set of row blocks.
	 * 
	 * @param mat
	 * @param numIterations
	 * @return
	 */
	private static MatrixData[] splitMatrixRowWise(MatrixData mat,
			int numIterations, int matNo) {
		int height = mat.getHeight();
		int rowHeight = height / numIterations;
		int rem = height % numIterations;

		MatrixData[] rows = new MatrixData[numIterations];
		double[][] data = mat.getData();
		double[][] row;
		int start = 0;
		int end = 0;
		int curHeight = 0;
		int count = 0;
		for (int i = 0; i < numIterations; i++) {
			end += rowHeight;
			if (rem > 0) {
				end++;
				rem--;
			}
			curHeight = end - start;
			row = new double[curHeight][mat.getWidth()];
			count = 0;
			for (int j = start; j < end; j++) {
				row[count][i] = data[j][i];
				count++;
			}
			rows[i] = new MatrixData(data, curHeight, mat.getWidth(), matNo);
			rows[i].setRow(i);
			start = end;
		}
		return rows;
	}

	private static boolean verify(double[][] A, double[][] B, double[][] C,
			int printCount) {
		int size = A.length;
		int verifyCount = A.length;

		boolean verified = true;

		double[][] D = new double[verifyCount][size];
		for (int i = 0; i < verifyCount; i++) {
			for (int j = 0; j < size; j++) {
				for (int k = 0; k < size; k++) {
					D[i][j] += A[i][k] * B[k][j];
				}
			}
		}

		for (int i = 0; i < printCount; i++) {
			for (int j = 0; j < size; j++) {
				System.out.println(D[i][j] + " " + C[i][j]);
				if (D[i][j] != C[i][j]) {
					verified = false;
				}
			}
		}
		return verified;
	}
}

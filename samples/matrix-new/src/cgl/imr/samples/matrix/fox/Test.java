package cgl.imr.samples.matrix.fox;

import cgl.imr.types.IntKey;

public class Test {

	    public static void show(double[][] a) {
	        int N = a.length;
	        for (int i = 0; i < N; i++) {
	            for (int j = 0; j < N; j++) {
	                System.out.printf("%6.4f ", a[i][j]);
	            }
	            System.out.println();
	        }
	        System.out.println();
	    }


	    public static void main(String[] args) {
	        int N = 1024;//Integer.parseInt(args[0]);
	        long start, stop;
	        double elapsed;


	        // generate input
	        start = System.currentTimeMillis(); 

	        double[][] A = new double[N][N];
	        double[][] B = new double[N][N];
	        double[][] C;

	        for (int i = 0; i < N; i++)
	            for (int j = 0; j < N; j++)
	                A[i][j] = Math.random();

	        for (int i = 0; i < N; i++)
	            for (int j = 0; j < N; j++)
	                B[i][j] = Math.random();

	        stop = System.currentTimeMillis();
	        elapsed = (stop - start) / 1000.0;
	        System.out.println("Generating input:  " + elapsed + " seconds");

	        // order 1: ijk = dot product version
	        C = new double[N][N];
	        start = System.currentTimeMillis(); 
	        for (int i = 0; i < N; i++)
	            for (int j = 0; j < N; j++)
	                for (int k = 0; k < N; k++)
	                    C[i][j] += A[i][k] * B[k][j];
	        stop = System.currentTimeMillis();
	        elapsed = (stop - start) / 1000.0;
	        System.out.println("Order ijk:   " + elapsed + " seconds");
	        if (N < 10) show(C);

	        // order 2: ikj
	        C = new double[N][N];
	        start = System.currentTimeMillis(); 
	        for (int i = 0; i < N; i++)
	            for (int k = 0; k < N; k++)
	                for (int j = 0; j < N; j++)
	                    C[i][j] += A[i][k] * B[k][j];
	        stop = System.currentTimeMillis();
	        elapsed = (stop - start) / 1000.0;
	        System.out.println("Order ikj:   " + elapsed + " seconds");
	        if (N < 10) show(C);

	        // order 3: jik
	        C = new double[N][N];
	        start = System.currentTimeMillis(); 
	        for (int j = 0; j < N; j++)
	            for (int i = 0; i < N; i++)
	                for (int k = 0; k < N; k++)
	                    C[i][j] += A[i][k] * B[k][j];
	        stop = System.currentTimeMillis();
	        elapsed = (stop - start) / 1000.0;
	        System.out.println("Order jik:   " + elapsed + " seconds");
	        if (N < 10) show(C);

	        // order 4: jki = GAXPY version
	        C = new double[N][N];
	        start = System.currentTimeMillis(); 
	        for (int j = 0; j < N; j++)
	            for (int k = 0; k < N; k++)
	                for (int i = 0; i < N; i++)
	                    C[i][j] += A[i][k] * B[k][j];
	        stop = System.currentTimeMillis();
	        elapsed = (stop - start) / 1000.0;
	        System.out.println("Order jki:   " + elapsed + " seconds");
	        if (N < 10) show(C);

	        // order 5: kij
	        C = new double[N][N];
	        start = System.currentTimeMillis(); 
	        for (int k = 0; k < N; k++)
	            for (int i = 0; i < N; i++)
	                for (int j = 0; j < N; j++)
	                    C[i][j] += A[i][k] * B[k][j];
	        stop = System.currentTimeMillis();
	        elapsed = (stop - start) / 1000.0;
	        System.out.println("Order kij:   " + elapsed + " seconds");
	        if (N < 10) show(C);

	        // order 6: kji = outer product version
	        C = new double[N][N];
	        start = System.currentTimeMillis(); 
	        for (int k = 0; k < N; k++)
	            for (int j = 0; j < N; j++)
	                for (int i = 0; i < N; i++)
	                    C[i][j] += A[i][k] * B[k][j];
	        stop = System.currentTimeMillis();
	        elapsed = (stop - start) / 1000.0;
	        System.out.println("Order kji:   " + elapsed + " seconds");
	        if (N < 10) show(C);


	        // order 7: jik optimized ala JAMA 
	        C = new double[N][N];
	        start = System.currentTimeMillis(); 
	        double[] bcolj = new double[N];
	        for (int j = 0; j < N; j++) {
	            for (int k = 0; k < N; k++) bcolj[k] = B[k][j];
	            for (int i = 0; i < N; i++) {
	                double[] arowi = A[i];
	                double sum = 0.0;
	                for (int k = 0; k < N; k++) {
	                    sum += arowi[k] * bcolj[k];
	                }
	                C[i][j] = sum;
	            }
	        }
	        stop = System.currentTimeMillis();
	        elapsed = (stop - start) / 1000.0;
	        System.out.println("Order jik JAMA optimized:   " + elapsed + " seconds");
	        if (N < 10) show(C);

	        // order 8: ikj pure row
	        C = new double[N][N];
	        start = System.currentTimeMillis(); 
	        for (int i = 0; i < N; i++) {
	            double[] arowi = A[i];
	            double[] crowi = C[i];
	            for (int k = 0; k < N; k++) {
	                double[] browk = B[k];
	                double aik = arowi[k];
	                for (int j = 0; j < N; j++) {
	                    crowi[j] += aik * browk[j];
	                }
	            }
	        }
	        stop = System.currentTimeMillis();
	        elapsed = (stop - start) / 1000.0;
	        System.out.println("Order ikj pure row:   " + elapsed + " seconds");
	        if (N < 10) show(C);

	    }

	}



	
	
	
	/*public static void main(String[] args) {
		int mapNo;
		int I;
		int n = 4;
		int k = 0;
		
		Integer ten=10;
		System.out.println(ten.toString());
		
		Integer tten=10;
		System.out.println(ten.toString().endsWith(tten.toString()));

		for (I = 0; I < n; I++) {

			for (mapNo = 0; mapNo < n * n; mapNo++) {

				int nMod = mapNo % n;
				int nDiv = mapNo / n;

				if (((nDiv + I) % n) == nMod) {
					// int A_RowReduceKeyStart=nDiv*n;
					// int A_RowReduceKeyEnd=A_RowReduceKeyStart+n;

					System.out.println("Map no " + mapNo + " bcasting A block "	+ nDiv);
					// collector.collectBCastToRow(nMod,new IntKey(nMod),
					// ABlock);
				}
			}

			System.out.println("Iteration " + I + "===========");
			for (mapNo = 0; mapNo < n * n; mapNo++) {
				//k = (mapNo + I * n) % (n * n);
				k = ((n-I)*n + mapNo) % (n * n);
				System.out.print(mapNo + " -> " + k + " ");
			}
			System.out.println();
		}
	}*/


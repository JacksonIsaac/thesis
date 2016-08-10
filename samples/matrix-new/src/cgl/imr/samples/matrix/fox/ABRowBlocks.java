package cgl.imr.samples.matrix.fox;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cgl.imr.base.SerializationException;
import cgl.imr.base.Value;

public class ABRowBlocks implements Value{
	MatrixData A_RowB;
	MatrixData B_RowB;
		
	public ABRowBlocks(){}
	
	public ABRowBlocks(MatrixData A_RowB, MatrixData B_RowB) {
		super();
		this.A_RowB = A_RowB;
		this.B_RowB = B_RowB;
	}
		
	public MatrixData getA_RowB() {
		return A_RowB;
	}

	public MatrixData getB_RowB() {
		return B_RowB;
	}

	public void fromBytes(byte[] bytes) throws SerializationException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {
			int len=0;
			byte[] data = null;
			len=din.readInt();
			data = new byte[len];
			din.readFully(data);
			A_RowB=new MatrixData(data);
			
			len=din.readInt();
			data = new byte[len];
			din.readFully(data);
			B_RowB=new MatrixData(data);	
		
			din.close();
			baInputStream.close();

		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
	}
	

	@Override
	public byte[] getBytes() throws SerializationException {
		byte[] serializedBytes = null;

		try {

			ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
			DataOutputStream dout = new DataOutputStream(baOutputStream);

			byte[] data = null;

			data=A_RowB.getBytes();
			dout.writeInt(data.length);
			dout.write(data);
			
			data=B_RowB.getBytes();
			dout.writeInt(data.length);
			dout.write(data);

			dout.flush();
			serializedBytes = baOutputStream.toByteArray();
			dout.close();
			baOutputStream.close();			
			baOutputStream = null;
			dout = null;

		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
		return serializedBytes;
	}
	
}

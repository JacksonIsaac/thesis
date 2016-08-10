package cgl.imr.samples.matrix.fox;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cgl.imr.base.SerializationException;
import cgl.imr.base.Value;

public class MapData implements Value {
	
	private MatrixData ABlock;
	private MatrixData BBlock;
	
	public MapData(){}
	
	public MapData(MatrixData a, MatrixData b){
		this.ABlock=a;
		this.BBlock=b;
	}	
	

	public MatrixData getABlock() {
		return ABlock;
	}



	public void setABlock(MatrixData aBlock) {
		ABlock = aBlock;
	}



	public MatrixData getBBlock() {
		return BBlock;
	}



	public void setBBlock(MatrixData bBlock) {
		BBlock = bBlock;
	}



	@Override
	public void fromBytes(byte[] bytes) throws SerializationException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);
		int len;
		byte[] data;
		try {
			len=din.readInt();
			data=new byte[len];
			din.read(data);
			ABlock= new MatrixData(data);
			len=din.readInt();
			data=new byte[len];
			din.read(data);
			BBlock= new MatrixData(data);			
			din.close();
			baInputStream.close();

		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
		
	}

	@Override
	public byte[] getBytes() throws SerializationException {
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();

		DataOutputStream dout = new DataOutputStream(baOutputStream);
		byte[] marshalledBytes = null;
		byte[] data;
		try {
			data=ABlock.getBytes();
			dout.writeInt(data.length);
			dout.write(data);
			data=BBlock.getBytes();
			dout.writeInt(data.length);
			dout.write(data);
			dout.flush();
			dout.close();
			baOutputStream.close();
			marshalledBytes = baOutputStream.toByteArray();
			baOutputStream = null;
			dout = null;
		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
		return marshalledBytes;
	}

}

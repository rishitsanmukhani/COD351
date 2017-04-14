package hbs;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AppendTest {
	
	public static void appendTest(){
		FileSystem fs = HbsUtil2.getFileSystem();
		try{
			Path filePath = new Path("/user/Hblock/testFile2");
			System.out.println(System.currentTimeMillis());

			fs.createNewFile(filePath);
			FSDataOutputStream fsout = fs.append(filePath);
			System.out.println(System.currentTimeMillis());

			for (int i=0; i<4000; i++){
				BlockData b = HbsUtil2.genDefaultBlockData(0, 1<<16);
				fsout.writeLong(13123);;
				fsout.write(b.value);
				fsout.hflush();
			}
			fsout.close();
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]){
//		bufferTest();
//		appendTest();
		guassianTest();
		System.out.println(System.currentTimeMillis());
	}
	
	private static void guassianTest(){
		Random r = new Random();
		for (int i=0; i<100; i++){
			double g = r.nextGaussian()*8.3333 + 125;
			System.out.print((int) g + " ");
			
		}
	}
	
	public static void bufferTest(){
		byte[] a = new byte[100];
		a[0] = 1;
		printByteArray(a);
		ByteBuffer b = ByteBuffer.wrap(a);
		b.putInt(256);
		b.putLong(2);
		byte[] a2 = b.array();
		printByteArray(a2);
		System.out.println(" ----------------- ");
		printByteArray(HbsUtil2.genDefaultBlockData2(0, 100).value);
		printByteArray(HbsUtil2.genDefaultBlockData(0, 100).value);
		
	}
	
	public static void printByteArray(byte[] b){
		ByteBuffer byteBuffer = ByteBuffer.wrap(b);
		System.out.println(byteBuffer.getInt());
//		System.out.println(byteBuffer.getLong());
		for (int i=0; i<b.length; i++){
			System.out.print(b[i] + " ");
		}
		System.out.println();
	}
	
}
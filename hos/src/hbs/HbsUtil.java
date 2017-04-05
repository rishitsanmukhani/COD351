package hbs;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import hbase.DBStrings;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;

public final class HbsUtil {
	
	public static final String FSNAME = "hdfs://master:9000/";
	private Random rng = new Random() ;
	public static String fileNamechars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	public static int maxFileNameSize = 64;
		
	public  void print(String s){
		System.out.println(s);
	}
	
	public void print(int n){
		System.out.println(n);
	}

	public byte[] longToByte(long n){
		ByteBuffer b = ByteBuffer.allocate(8);
		//b.order(ByteOrder.BIG_ENDIAN); // optional, the initial order of a byte buffer is always BIG_ENDIAN.
		b.putLong(n);

		byte[] result = b.array();
		return result;
	}

	public long byteToLong(byte[] b){
		return ByteBuffer.wrap(b).getLong();
	}
	
	public String generateString()
	{
		int length = rng.nextInt(maxFileNameSize-2) + 2;
	    char[] text = new char[length];
	    for (int i = 0; i < length; i++)
	    {
	        text[i] = fileNamechars.charAt(rng.nextInt(fileNamechars.length()));
	    }
	    return new String(text);
	}

	
	public boolean folderExists(FileSystem fs, String dirName) throws IOException {
		Path p = new Path(dirName);
		if (fs.exists(p)){
			return true;
		}
		return false;			
	}
	
	public Configuration getConfiguration(){
		Configuration config = new Configuration();
//		System.setProperty("HADOOP_USER_NAME", "hduser");
		config.set("fs.default.name", FSNAME);
		config.setInt("dfs.replication", 1);
		return config;
	}
	
	public FileSystem getFileSystem(){
		FileSystem fs = null;
		Configuration config = getConfiguration();
		try {
			fs = FileSystem.get(config);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fs;
	}

	
	public HarFileSystem getHarFileSystem(){
		HarFileSystem hfs = new HarFileSystem(getFileSystem());
		return hfs;
	}
	
	public boolean attrCheck(FileSystem fs, String dirName, String attr){
		try{
			Object[] attrList = fs.listXAttrs(new Path(dirName)).toArray();
			for (int i=0; i<attrList.length; i++){
				if (attrList[i].toString().equals(attr)){
					return true;
				}
			}
		}
		catch (Exception e){
			e.printStackTrace();
		}
		return false;
	}
	
	public static byte[] intToByte(int n){
		ByteBuffer b = ByteBuffer.allocate(4);
		//b.order(ByteOrder.BIG_ENDIAN); // optional, the initial order of a byte buffer is always BIG_ENDIAN.
		b.putInt(n);

		byte[] result = b.array();
		return result;
	}

	public static int byteToInt(byte[] b){
		return ByteBuffer.wrap(b).getInt();
	}

	public long getImageSize(FileSystem fs, String dirName){
		try{
			byte[]  b = fs.getXAttr(new Path(dirName), "user.size");
			return ByteBuffer.wrap(b).getLong();
		}
		catch (Exception e){
			e.printStackTrace();
		}
		return -1;
	}

	
//	 new util functions from here
	
	public BlockData genBlockData(long addr){
		BlockData bd = new BlockData();
//		22*hadoopBlockSize
		bd.addr = addr;
		bd.value = new byte[(int) BlockProtocol.clientBlockSize];
		bd.value[1] = 1;
		bd.value[6] = 1;
		return bd;
	}
	
	public String genDefaultUserPath(String s){
		return "/user/" + DBStrings.USER_NAME + "/" + s; 
	}
	
	
}

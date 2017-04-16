package hbs;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HbsUtil2 {
	public static final String FSNAME = "hdfs://master:9000/";
	private Random rng = new Random() ;
	public static String fileNamechars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	public static int maxFileNameSize = 32;
		
	public String generateString()
	{
		int length = rng.nextInt(maxFileNameSize -2) + 2;
	    char[] text = new char[length];
	    for (int i = 0; i < length; i++)
	    {
	        text[i] = fileNamechars.charAt(rng.nextInt(fileNamechars.length()));
	    }
	    return new String(text);
	}
	
	public static BlockData genDefaultBlockData(long addr, long clientBlockSize){
		BlockData bd = new BlockData();
		bd.addr = addr;
		bd.value = new byte[(int) clientBlockSize];
		bd.value[3] = 1;
		return bd;
	}

	public static BlockData genDefaultBlockData2(long addr, long clientBlockSize){
		BlockData bd = new BlockData();
		bd.addr = addr;
		bd.value = new byte[(int) clientBlockSize];
		bd.value[4] = 3;
		return bd;
	}
	
	public static Configuration getConfiguration(){
		Configuration config = new Configuration();
		System.setProperty("HADOOP_USER_NAME", "rishit");
		config.set("fs.defaultFS", FSNAME);
		config.setInt("dfs.replication", 2);
		return config;
	}
	
	public static FileSystem getFileSystem(){
		FileSystem fs = null;
		Configuration config = getConfiguration();
		try {
			fs = FileSystem.get(config);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fs;
	}

	public static void printBlockData(BlockData b){
		System.out.print(b.addr + " ");
		for (int i=0; i<b.value.length; i++){
			System.out.print(b.value[i] + ",");
		}
		System.out.println();
	}
	
}

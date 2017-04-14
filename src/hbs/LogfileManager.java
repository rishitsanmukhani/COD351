package hbs;

import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LogfileManager {
	FileSystem fs;
	private String LOG_FILENAME = "/logFile";
	HbsUtil utilities = new HbsUtil();
	FSDataOutputStream appendStream;
	HashMap<Long, BlockData> logMem = new HashMap<Long, BlockData>();
	
	public LogfileManager(FileSystem fs1){
		fs = fs1;
	}
	
	public void init(String imageId){
		Path logPath = new Path(imageId + LOG_FILENAME);
		try {
			if (!fs.exists(logPath)){
//				System.out.println("Path doesn't exist");
				fs.createNewFile(logPath);
			}
			appendStream = fs.append(logPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
		loadLogFile(imageId);
	}
	
	public void close(){
		try {
			appendStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		logMem.clear();
	}
	
	public void loadLogFile(String imageKey){
		try {
			FSDataInputStream fsin = utilities.getFileSystem().open((new Path(imageKey + "/logFile")));
			long len = fs.getFileStatus((new Path(imageKey + "/logFile"))).getLen();
			long caddr;
			try{
					byte[] addr_byte = new byte[8];
					byte[] tot = new byte[(int)len];
					fsin.readFully(0, tot, 0, (int)len);
					int unitSize = (int) (BlockProtocol.clientBlockSize + 8);
					int nEle = (int) (len/unitSize);
					int start;
					for (int ind = 0; ind<nEle ; ind++){
						BlockData bd = utilities.genBlockData(-1);
						start = ind*unitSize;
						addr_byte = Arrays.copyOfRange(tot, start, start+8);
						caddr = utilities.byteToLong(addr_byte);
						bd.addr = caddr;
						bd.value = Arrays.copyOfRange(tot, start+8, start+unitSize);
						logMem.put(bd.addr, bd);
					}
				}
				catch (EOFException e){
					e.printStackTrace();
				}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public BlockData readLogFile(String imageId, long addr){
		if (logMem.containsKey(addr)){
			return logMem.get(addr);
		}
		BlockData b = utilities.genBlockData(-1);
		return b;
	}
	
	/**
	 * This function appends the small change to the log file, which will be committed later
	 * @param imageId
	 * @param block
	 * @return
	 */
	public boolean appendtoLog(String imageId, BlockData block){
		Path logPath = new Path(imageId + LOG_FILENAME);
		try {
//			if (!fs.exists(logPath)){
//				System.out.println("Path doesn't exist");
//				fs.createNewFile(logPath);
//			}
			
//			System.out.println("writing : " + block.addr);
			logMem.put(block.addr, block);
			appendStream.writeLong(block.addr);
			appendStream.write(block.value);
			appendStream.hflush();
//			appendStream.close();

//			FSDataInputStream fsin = utilities.getFileSystem().open((new Path(imageId + "/logFile")));
//			byte[] b = new byte[65664];
//			fsin.read(b);
//			FileOutputStream out = new FileOutputStream("outputLog");
//			out.write(b);
//			out.close();
//
			
			
//			appendStream.hsync();
			
		} catch (IOException e) {
			System.out.println("fs exception in append to log");
			e.printStackTrace();
			return false;
		}
		return true;
	}
	

	/**
	 * 
	 * This function reads the corresponding data for the address addr 
	 *  from the image corresponding to imageKey
	 * @param imageKey
	 * @param addr
	 * @return BlockData
	 */
	public BlockData readFromLog(String imageKey, long addr){
		BlockData bd = utilities.genBlockData(-1), bdStore = utilities.genBlockData(-1);
		
		try {
			FSDataInputStream fsin = utilities.getFileSystem().open((new Path(imageKey + "/logFile")));
			long caddr;
			while(true){
				try{
					caddr = fsin.readLong();
					if (caddr==addr){
						fsin.read(bd.value);
					}
					else{
						fsin.read(bdStore.value);
					}
				}
				catch (EOFException e){
					return bd;
				}
			}
		} catch (IOException e) {
			System.out.println("fs exception in readfromLog");
			e.printStackTrace();
		}
		return bd;
	}
	
	/**
	 * This function takes the data from block and writes it on the hadoop cluster.
	 * @param imageId
	 * @param block
	 * @return boolean
	 */
//	TODO Fix this
	public boolean flushLog(String imageId, BlockData block ){

		long addr = block.addr/BlockProtocol.hadoopBlockSize;
		long offset = block.addr%BlockProtocol.hadoopBlockSize;
		try {
			FSDataOutputStream fsst1;
			byte[] b = new byte[(int) BlockProtocol.hadoopBlockSize];
			if (fs.exists(new Path(imageId + "/" + String.valueOf(addr))))
			{
				FSDataInputStream fsst = fs.open(new Path(imageId + "/" + String.valueOf(addr)));
				fsst.readFully(b);
				fsst.close();
				fs.delete(new Path(imageId + "/" + String.valueOf(addr)),true);
			}
			else
			{
				System.out.println("unable to find corresponding image");
				return false;
			}
			for(long i = offset; i < offset + BlockProtocol.clientBlockSize ; i++ ){
				b[(int)i] = block.value[(int)(i-offset)];
			} 
			fsst1 = fs.create(new Path(imageId + "/" + String.valueOf(addr)) );
			fsst1.write(b);
			fsst1.close();	
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
	
	
}

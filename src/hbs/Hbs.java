package hbs;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.ProtocolSignature;

public class Hbs implements BlockProtocol {

	HbsUtil utilities = new HbsUtil();
	public FileSystem fs = utilities.getFileSystem();
	LogfileManager lfm = new LogfileManager(fs);
	long oldFileIndex = -1;
	FSDataInputStream oldDis;
	
	@Override
	public ProtocolSignature getProtocolSignature(String arg0, long arg1, int arg2) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		// TODO Auto-generated method stub
		return versionID;
	}

	@Override
	public File getImage(String imageKey) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void initForImage(String imageKey){
		lfm.init(imageKey);
	}

	@Override
	public boolean writeBlock(String imageKey, long addr, BlockData block) {
		try{
//			if (utilities.folderExists(fs, imageKey)){
				if (utilities.attrCheck(fs, imageKey, "user.tail")){
					System.out.println("has tail");
				// check if tail has the block
					String tailImageId = new String(fs.getXAttr(new Path(imageKey), "user.tail"));
					BlockData tailBlock = readFromFolder(fs, tailImageId, addr);
					if (tailBlock.addr != addr){
						// tail doesn't have the block => add the current one to it.
						BlockData currBlock= readFromFolder(fs, imageKey, addr);
						writeBlock(tailImageId, addr, currBlock);
					}
				}
//				System.out.println("appending");
				lfm.appendtoLog(imageKey, block);
				return true;
//			}
//			else{
//				utilities.print("No such folder");
//			}	
		}
		catch (Exception e){
			utilities.print(" Exception raised in writeBlock");
			e.printStackTrace();
		}
		return false;
	}
	@Override
	public List<BlockData> readBlock(String imageKey, long addr) {
		List<BlockData> ret = new ArrayList<BlockData>();
		BlockData res1 = readFromFolder(fs, imageKey, addr);
		if (res1.addr == addr){
			ret.add(res1);
			return ret;
		}
		else{
			boolean headExist = utilities.attrCheck(fs, imageKey, "user.head");
			if (headExist){
				try{
					String headVal = new String(fs.getXAttr(new Path(imageKey), "user.head"));
					return readBlock(headVal, addr);
				}
				catch (Exception e1){
					e1.printStackTrace();
				}
			}
			else{
				//utilities.print("Cannot find any refernce to read");
				BlockData defblock = utilities.genBlockData(addr);
				ret.add(defblock);
				return ret;
			}			
		}
		return null;
	}

	@Override
	public String takeSnapShot(String imageKey) {
		long size = utilities.getImageSize(fs, imageKey);
		size = size*BlockProtocol.hadoopBlockSize/(1<<20);
		String newImageId = createImage(size);
		if (utilities.attrCheck(fs, imageKey, "user.tail")){
			try{
				String tailImageId = new String(fs.getXAttr(new Path(imageKey), "user.tail"));
				fs.setXAttr(new Path(tailImageId), "user.head", newImageId.getBytes());
				fs.setXAttr(new Path(newImageId), "user.tail", tailImageId.getBytes());
			}
			catch (Exception e){
				utilities.print("error in part 1 taking snap shot");
				e.printStackTrace();
			}
		}
		// if doesn't contain tail, create a tail
		try{
			fs.setXAttr(new Path(imageKey), "user.tail", newImageId.getBytes());
			fs.setXAttr(new Path(newImageId), "user.head", imageKey.getBytes());
		}
		catch (Exception e){
			utilities.print("error in part 2 taking snap shot");
			e.printStackTrace();
		}
		return null;
	}
	
//	imageSize is in MB
	@Override
	public String createImage(long imageSize){
		imageSize = (((imageSize -1)*(1<<20))/BlockProtocol.hadoopBlockSize + 1 ) ;
		String newFolderName = utilities.generateString();
		boolean status;
		//FQDN and check path and properties
		try
		{
			Path newPath = new Path(newFolderName);
			while( fs.exists(newPath) )
			{
				newFolderName = utilities.generateString();
				newPath = new Path(newFolderName);
			}
			status = fs.mkdirs(newPath);
			fs.setXAttr(newPath,  "user.size", utilities.longToByte(imageSize));	
			//TODO createimg file.
		}
		catch(IOException e)
		{
			return null;
		}
		if (status == false){
			System.err.println("Create Image of size " + imageSize + " failed. Folder name = " + newFolderName);
			return null;
		}
		initForImage(newFolderName);
		return newFolderName;
	}
	
	@Override
	public boolean extendImage(String imageKey, long newSize) {
		newSize = (((newSize -1)*(1<<20))/BlockProtocol.hadoopBlockSize + 1 );
		Path imagePath = new Path(imageKey);
		try
		{
			if(fs.exists(imagePath))
			{
				if( utilities.byteToLong(fs.getXAttr(imagePath, "user.size") ) <= newSize )
				{
					fs.setXAttr(imagePath,  "user.size", utilities.longToByte(newSize));					
				}
				else
				{
					System.err.println("Extend Image failed, New size smaller than the present size");
				}
			}
			else
			{
				System.err.println("Extend Image failed, no such path exists");
			}
			return true;
		}
		catch(IOException e){
			System.err.println("Extend Image failed");
			return false;
		}
	}

	@Override
	public boolean trimImage(String imageKey, long newSize) {
		newSize = (((newSize -1)*(1<<20))/BlockProtocol.hadoopBlockSize + 1 );
		Path imagePath = new Path(imageKey);
		try{
			if(fs.exists(imagePath))
			{
				long currSize = utilities.byteToLong(fs.getXAttr(imagePath, "user.size") ); 
				if( currSize > newSize )
				{
					fs.setXAttr(imagePath,  "user.size", utilities.longToByte(newSize));
					long fileIndex = newSize;
					while(fileIndex < currSize )
					{
						fs.delete( new Path(imageKey + "/" + fileIndex), false );	
					}
				}
				else
				{
					System.out.println("Nothing to trim. New size greater than the present size");
				}
			}
			else
			{
				System.err.println("Trim Image failed, no such path exists");
			}
			return true;
		}
		catch(IOException e)
		{				
			System.err.println("Trim Image failed");
			return false;
		}
		
	}
	
	@Override
	public boolean deleteImage(String imageKey) {
		try
		{
			if(fs.exists(new Path(imageKey)) )
			{
				fs.delete( new Path(imageKey), true);	
				return true;
			}	
		}
		catch( IOException e){
			System.err.println("Delete failed, " + imageKey);
			return false;
		}
		System.err.println("No such Image, " + imageKey);
		return true;
		
	}
		

	public void addEntry(HashMap<Long, List<BlockData> > h, BlockData b){
		long hadoopBlockNo = b.addr/BlockProtocol.hadoopBlockSize;
		if (!h.containsKey(hadoopBlockNo)){
			h.put(hadoopBlockNo, new ArrayList());
		}
		h.get(hadoopBlockNo).add(b);
//		System.out.println("adding " + b.addr + " for " + hadoopBlockNo);
//		printAddr(h.get(hadoopBlockNo));

	}
	
	public void printAddr(List a){
		for (int i=0; i<a.size(); i++){
			BlockData b = (BlockData) a.get(i);
			System.out.print (" " + b.addr);
		}
		System.out.println();
	}
	
	@Override
	public boolean commit(String imageKey){
		// load full Log File
		// tabulate all required Files
		try {
			// clear data
			lfm.appendStream.close();
			lfm.logMem.clear();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		FSDataInputStream fsin = null;
		HashMap<Long, List<BlockData> > infoChanges = null;
		try {
			fsin = utilities.getFileSystem().open((new Path(imageKey + "/logFile")));
//			byte[] b = new byte[2101248];
//			byte[] b = new byte[2<<20];
//			fsin.readFully(b);
//			FileOutputStream out = new FileOutputStream("outputLog");
//			out.write(b);
//			out.close();
//			System.out.println(Arrays.toString(b));

			long len = fs.getFileStatus((new Path(imageKey + "/logFile"))).getLen();
//			System.out.println("len : " + len);
			infoChanges = new HashMap<Long, List<BlockData> >();
			long caddr;
//			while(true){
				try{
//						caddr = fsin.readLong();
//						System.out.println("reading : " + caddr);
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
//							System.out.println("index : " + ind + " addr: "+ caddr);
//							p.write("ind : " + ind + " value: " + Arrays.toString(bd.value));
							addEntry(infoChanges, bd);
						}
//						p.write(Arrays.toString(tot));
//						if (ret)
//						int ret1 = fsin.read(0, addr_byte, 0, 8);
//						if (ret1==-1) break;
//						
//						bd.addr = caddr;
//						int ret2 = fsin.read(bd.value);
//						if (ret2==-1) break;
//						p.write("writing at : ");
//						p.write(Arrays.toString(addr_byte));
//						p.write(" as : " + Arrays.toString(bd.value) + "\n");

//						System.out.println("ret1: " + ret1 + " addr: " + Arrays.toString(addr_byte) + " = " + caddr + " ret2 : " + ret2);
//						System.out.println("ret: " + ret + " addr: " + Arrays.toString(addr_byte) + " value: " + Arrays.toString(bd.value));
						
				}
				catch (EOFException e){
					// reading from Log file is done
					e.printStackTrace();
//					break;
				}
//			}
//			p.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		
		
		// iterate over infoChanges and get each hadoopBlock, update it and send it back
	      Set set = infoChanges.entrySet();
	      Iterator i = set.iterator();
	      while(i.hasNext()) {
	         Map.Entry me = (Map.Entry)i.next();
	         long hadoopBlockNo = (long) me.getKey();
	       
			byte[] newBlock = new byte[(int) BlockProtocol.hadoopBlockSize];
			Path blockPath = new Path(imageKey + "/" + String.valueOf(hadoopBlockNo));
			try {
//				read old data into b
				if (fs.exists(blockPath)){
					FSDataInputStream fsst = fs.open(blockPath);
					fsst.readFully(newBlock);
					fsst.close();
					fs.delete(blockPath, true);
				}
				// if the path doesn't exist, no need to get any data
				// update b with new modifications
		         ArrayList changes = (ArrayList) me.getValue();
//		         System.out.println("changes: ");
//		         printAddr(changes);
//		         System.out.println("len: " + changes.size());
		         for (int ind = 0; ind<changes.size(); ind++){
		        	 BlockData newChange =(BlockData) changes.get(ind);
		     		long offset = newChange.addr%BlockProtocol.hadoopBlockSize;
					// updating b
//		     		System.out.println("addr: " + newChange.addr);
		     		for(long counter = offset; counter < offset + BlockProtocol.clientBlockSize ; counter++ ){
						newBlock[(int) counter] = newChange.value[(int)(counter-offset)];
					}
		         }
		         
					FSDataOutputStream fsst1;
					fsst1 = fs.create(blockPath, true, 4096, (short) 3, BlockProtocol.hadoopBlockSize);
					fsst1.write(newBlock);
					fsst1.close();	
					
				
			} catch (Exception e) {
				e.printStackTrace();
			}
	      }
	      
	      try{
	    	  fs.delete(new Path(imageKey + "/logFile"), false);
	    	  lfm.init(imageKey);
	      } catch (Exception e){
	    	  e.printStackTrace();
	      }
	      
	      return true;
	}

	public void close(){
		try {
			if (oldDis != null){
				oldDis.close();				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		lfm.close();
		oldFileIndex = -1;
	}

	public BlockData readFromFolder(FileSystem fs, String imageKey, long addr){
		// if the current folder doesn't have the corr addr, returns wrong addr as result
		// TODO Check the above
//		System.out.println("starting read for " + addr);
		BlockData fromLog = lfm.readLogFile(imageKey, addr);
//		System.out.println(addr);
		if ( fromLog.addr == addr){
//			System.out.println("returning from log: " + Arrays.toString(fromLog.value));
			return fromLog;
		}
				
		try{
			long fileIndex = addr/BlockProtocol.hadoopBlockSize;
			Path filePath;
			FSDataInputStream dis;
			
			if (oldFileIndex == fileIndex){
				System.out.println("re using old input stream");
				dis = oldDis;
			}
			else{
				filePath = new Path(imageKey +"/" + String.valueOf(fileIndex));
				dis = fs.open(filePath);
				oldFileIndex = fileIndex;
//				System.out.println("last not valid. Creating new path for " + fileIndex);
				oldDis = dis;
			}
			
			int clientBlockSize = (int) BlockProtocol.clientBlockSize;
			
		/*
		 * TODO  - 	Fix this read with a while loop. Read documentation if not clear.
		 * 			Read may not read all the values.
		 * 			Read may fail unexpectedly depending on its mood.
		 * */			
			dis.read(addr%BlockProtocol.hadoopBlockSize, fromLog.value, 0, clientBlockSize);
//			System.out.println("value : " + Arrays.toString(fromLog.value));
			fromLog.addr = addr;
		}
		catch (Exception e){
			e.printStackTrace();
			fromLog.addr = -1;
		}
		return fromLog;
	}
	
	
	
}
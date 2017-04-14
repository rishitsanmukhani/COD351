package hbs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Currency;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ImageMemMeta {
	
	public long clientBlockSize ;
	public long hadoopBlockSize ;
	public long hadooptempBlockSize ;

	
	public FSDataOutputStream currentAppendStream;
	public long currentAppendStreamPos = 0;
	public FSDataOutputStream tempAppendStream;

	public short currentFile;
	public ArrayList<FSDataInputStream> readStreams;
	public long maxFileSize  ;
	public short maxFileNumber;
	public String imageKey;
	public String imageUser;
	public long imageSize;

	public MaxSizeHashMap<Long, BlockData> readCache;
	private int USER_CACHE_LIMIT = 1000;
	public MaxSizeHashMap<Long, WriteCacheValue> writeCache;
	public int writeCacheNumElements = 0;
	public int WRITE_CACHE_LIMIT = 200;
	private static String tempPrefix = "_temp";

	public Map<Long, AddrDataMapValue > blocksDirectory;
	public static HblockConnectionManager hblockConnectionManager;
		
	// testing purposes
	public int readCacheHit = 0, writeCacheHit = 0;
	
	
	public void closeAppendStream(){
		if (currentAppendStream == null) return;
		try{
			currentAppendStream.close();			
			currentAppendStream = null;
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
	public void openAppendStream(){
		if (currentAppendStream !=null) return;
		try{
			Path currPath = new Path(hblockConnectionManager.prefix + imageUser + "/" + imageKey + "/" + currentFile );
			currentAppendStream = hblockConnectionManager.fs.append(currPath);
			currentAppendStreamPos = currentAppendStream.getPos();
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
	public void reOpenAppendStream(){
		closeAppendStream();
		openAppendStream();
	}
	
	public void openTempAppendStream(){
		try{
			Path currPath = new Path(hblockConnectionManager.prefix + imageUser + "/" + imageKey + "/" + currentFile + tempPrefix );
			tempAppendStream = hblockConnectionManager.fs.append(currPath);
		} catch (Exception e){
			e.printStackTrace();
		}
	}

	public void commitTempUpdates(){
		if (writeCacheNumElements ==0) return;
		try{
			readStreams.get(currentFile).close();
			
			tempAppendStream.close();
			tempAppendStream = null;
			Map<Long, AddrDataMapValue > blocksDirectoryChanges =  new HashMap<Long, AddrDataMapValue>(WRITE_CACHE_LIMIT);
			
			if(currentAppendStream != null){
				System.out.println("Something wrong, appendStream open before commit");
			}
			openAppendStream();
			byte[] bufferArray = new byte[(int) (writeCacheNumElements*(12+clientBlockSize))];
			ByteBuffer byteBuffer = ByteBuffer.wrap(bufferArray);
			long currPos = currentAppendStream.getPos();
			for (Map.Entry<Long, WriteCacheValue> entry : writeCache.entrySet())
			{
				long a = entry.getKey();
				byteBuffer.putLong(a);
//				currentAppendStream.writeLong(a);
//				currentAppendStream.write(entry.getValue().value);
				int seqNumber = entry.getValue().seqNumber;
				byteBuffer.putInt(seqNumber);
				byteBuffer.put(entry.getValue().blockData.value);
				AddrDataMapValue b = new AddrDataMapValue(currentFile, seqNumber, currPos);
				currPos += (12+clientBlockSize);
				blocksDirectoryChanges.put(a,b);
			}
			currentAppendStream.write(byteBuffer.array());
			currentAppendStreamPos = currPos; //currentAppendStream.getPos();
			closeAppendStream();
			blocksDirectory.putAll(blocksDirectoryChanges);
			writeCache.clear();
			writeCacheNumElements = 0;
			Path tempPath = new Path(hblockConnectionManager.prefix + imageUser + "/" + imageKey + "/" + currentFile + tempPrefix );
//			hblockConnectionManager.fs.delete(tempPath, true);
			createFile(tempPath, hadooptempBlockSize);
			
			Path oldPath = new Path(hblockConnectionManager.prefix + imageUser + "/" + imageKey + "/" + currentFile );
			readStreams.add(currentFile, hblockConnectionManager.fs.open(oldPath));
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
	
	public void createFile(Path p, long blocksize){
		try {
			hblockConnectionManager.fs.create(p, true, 1<<20,
					hblockConnectionManager.fs.getDefaultReplication(hblockConnectionManager.rootFolderPath), blocksize).close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
	public ImageMemMeta(String imagekey, String imageuser, long maxfilesize, short maxfilenumber, long imagesize, long clientblocksize, long hadoopblocksize,
				HblockConnectionManager imagemanager, boolean inRecovery,String imageDir, ArrayList<String> dataFileList, short currFileRecovery){
		imageKey = imagekey;
		imageUser = imageuser;
		imageSize = imagesize;
		hblockConnectionManager = imagemanager;
		maxFileSize = maxfilesize;
		maxFileNumber = maxfilenumber;
		clientBlockSize = clientblocksize;
		hadoopBlockSize = hadoopblocksize;
		hadooptempBlockSize = hadoopblocksize;
		//hadooptempBlockSize = clientblocksize;
		System.out.println("setting image Size: " + imagesize);
		readStreams = new ArrayList<FSDataInputStream>(maxFileNumber);
		writeCacheNumElements = 0;
		currentFile = 0;
		currentAppendStreamPos = 0;

		if (inRecovery){
			for (int i=0; i<dataFileList.size(); i++){
				if (dataFileList.get(i).contains("_temp")) continue;
				Path dataFilePath = new Path(imageDir + "/" + dataFileList.get(i));
				try {
					readStreams.add(Integer.parseInt(dataFileList.get(i)), hblockConnectionManager.fs.open(dataFilePath));
				} catch (Exception e){
					e.printStackTrace();
				}
				currentFile = currFileRecovery;
			}
			Path tempPath = new Path(hblockConnectionManager.prefix + imageUser + "/" + imageKey + "/" + currentFile + tempPrefix);
			createFile(tempPath, hadooptempBlockSize);
			openTempAppendStream();
			Path currPath = new Path(hblockConnectionManager.prefix + imageUser + "/" + imageKey + "/" + currentFile );
			try {
				FileStatus currStatus = hblockConnectionManager.fs.getFileStatus(currPath);
				currentAppendStreamPos = currStatus.getLen();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else {
			Path currPath = new Path(hblockConnectionManager.prefix + imageUser + "/" + imageKey + "/" + currentFile );
			Path tempPath = new Path(hblockConnectionManager.prefix + imageUser + "/" + imageKey + "/" + currentFile + tempPrefix);
			
			try {
				createFile(currPath, hadoopBlockSize);
				createFile(tempPath, hadooptempBlockSize);
				readStreams.add(currentFile, hblockConnectionManager.fs.open(currPath));
				openTempAppendStream();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
		
		blocksDirectory = new HashMap<Long, AddrDataMapValue>();
		readCache = new MaxSizeHashMap<Long, BlockData>(USER_CACHE_LIMIT);
		writeCache = new MaxSizeHashMap<Long, WriteCacheValue>(WRITE_CACHE_LIMIT);
	}
	
	public void SetNextAppendStream() throws IOException{
		this.currentAppendStream.close();
		this.tempAppendStream.close();
		Path p22 = new Path(hblockConnectionManager.prefix + imageUser + "/" + imageKey + "/" + currentFile + tempPrefix );
		hblockConnectionManager.fs.delete(p22,false);
		Path oldPath = new Path(hblockConnectionManager.prefix + imageUser + "/" + imageKey + "/" + currentFile );
		readStreams.add(currentFile, hblockConnectionManager.fs.open(oldPath));
		int i;boolean found = false;
		for( i=0; i< maxFileNumber; i++){
			if(readStreams.get(i) == null ){
				found = true;
				break;
			}
		}
		if(found = true){
			Path p = new Path(hblockConnectionManager.prefix + imageUser + "/" + imageKey + "/" + i );
			Path p2 = new Path(hblockConnectionManager.prefix + imageUser + "/" + imageKey + "/" + i + tempPrefix );
			createFile(p, hadoopBlockSize);
			createFile(p2, hadooptempBlockSize);
//			currentAppendStream =hblockConnectionManager.fs.create(p); 
			currentFile = (short)i;	
		}
		else{
			//TODO try to commit
			for( i=0; ; i++){
				i = i%maxFileNumber;
				if(readStreams.get(i) == null ){
					found = true;
					break;
				}
			}
		}
	}
	
}

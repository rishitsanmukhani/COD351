package hbs;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsAdmin;

public class HblockConnectionManager {
	
	private Map<String, ImageMemMeta> imagesDirectory;
	public String prefix = null;
	public Path rootFolderPath;
	HbsUtil2 util = new HbsUtil2();
	public HdfsAdmin hdfsAdmin ; 
	public FileSystem fs ;
	private String PATH_SEPERATOR = "/";
	public static long DEFAULT_CLIENT_BLOCK_SIZE = 1<<16;
	public static long DEFAULT_HADOOP_BLOCK_SIZE = 1<<27;
	public static long DEFAULT_IMAGE_SIZE = 2L<<30;
	
	private void setPrefix(){
		prefix = "/user/Hblock/v0.5/";
	}
	
	public HblockConnectionManager(){
		setPrefix();
		fs =  HbsUtil2.getFileSystem();
		rootFolderPath = new Path(prefix);
		URI a  = HbsUtil2.getFileSystem().getUri();
		 try {
			hdfsAdmin = new HdfsAdmin(a,HbsUtil2.getConfiguration() );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		imagesDirectory = new HashMap<String, ImageMemMeta>();
		// call recover only at the end of this constructor
		recover();
	}
	
	public HblockConnectionManager(boolean startRecovery){
		setPrefix();
		fs =  HbsUtil2.getFileSystem();
		rootFolderPath = new Path(prefix);
		URI a  = HbsUtil2.getFileSystem().getUri();
		 try {
			hdfsAdmin = new HdfsAdmin(a,HbsUtil2.getConfiguration() );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		imagesDirectory = new HashMap<String, ImageMemMeta>();
		if (startRecovery){
			recover();
		}
	}

	public ArrayList<String> getFilesList(String parentDir){
		
		ArrayList<String> ret = new ArrayList<String>();
		try {
			FileStatus[] status = fs.listStatus(new Path(parentDir));
			for (int i=0;i<status.length;i++){
				String filePath = (status[i].getPath().toString());
				int index = filePath.lastIndexOf('/');
				ret.add(filePath.substring(index+1));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}
	
	public ArrayList<Long> getSizesList(String parentDir){
		ArrayList<Long> ret = new ArrayList<Long>();
		try {
			FileStatus[] status = fs.listStatus(new Path(parentDir));
			for (int i=0;i<status.length;i++){
				ret.add(status[i].getLen());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	} 

	private int findAnyTempFiles(ArrayList<String> fileList){
		for (int i=0; i<fileList.size(); i++){
			int tempFileNumberIndex = fileList.get(i).indexOf("_temp");
			if (tempFileNumberIndex == -1) continue;
			else{
				int tempFileNumber = Integer.parseInt(fileList.get(i).substring(0, tempFileNumberIndex));
				return tempFileNumber;
			}
		}
		return -1;
	}
	
	private void appendTempFile(String imageDir, int fileNumber){
		Path tempFilePath = new Path(imageDir + PATH_SEPERATOR + fileNumber + "_temp");
		Path dataFilePath = new Path(imageDir + PATH_SEPERATOR + fileNumber);
		try {
			FileStatus f = fs.getFileStatus(tempFilePath);
			long tempFileLen = f.getLen();
			byte[] buffer = new byte[(int)tempFileLen];
			FSDataInputStream tempStream = fs.open(tempFilePath);
			tempStream.readFully(buffer);
			tempStream.close();
			FSDataOutputStream dataFileStream = fs.append(dataFilePath);
			dataFileStream.write(buffer);
			dataFileStream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void recover(){
		// change this if required
		// 8 bytes for address, 4 bytes for seq number
		long unitSize = 12 + DEFAULT_CLIENT_BLOCK_SIZE;
		System.out.println("starting recovery");
		if (prefix == null) setPrefix();
		ArrayList<String> userList = getFilesList(prefix);
		for (int i=0; i<userList.size(); i++){
			String userDir = prefix + userList.get(i);
			ArrayList<String> imageList = getFilesList(userDir);
			System.out.println("for user " + userList.get(i));
			for (int j=0; j<imageList.size(); j++){
				System.out.println("	for image " + imageList.get(j));
				String imageDir = userDir + PATH_SEPERATOR + imageList.get(j);
				ArrayList<String> dataFileList = getFilesList(imageDir);
				ArrayList<Long> dataFileSizeList= getSizesList(imageDir) ;
				int tempFileNumber = findAnyTempFiles(dataFileList);
				System.out.println("		found temp file for data file " + tempFileNumber);
				// append any temp files
				if (tempFileNumber != -1){
					System.out.println("			appending temp file");
					appendTempFile(imageDir, tempFileNumber);
					try {
						fs.delete(new Path(imageDir + PATH_SEPERATOR + tempFileNumber + "_temp"), true);
						System.out.println("			deleted temp file: " + imageDir + PATH_SEPERATOR + tempFileNumber + "_temp");
					} catch (Exception e) {
						System.out.println("			unable to delete temp file");
						e.printStackTrace();
					}					
				}
				short currentFile = 0;
				if (tempFileNumber != -1) currentFile = (short) tempFileNumber;
				else{
					for (int k=0; k<dataFileSizeList.size(); k++){
						if (dataFileSizeList.get(k)<(DEFAULT_IMAGE_SIZE - 12 - DEFAULT_CLIENT_BLOCK_SIZE)){
							currentFile = (short)k;
							break;
						}
					}
				}
				System.out.println("		current file is " + currentFile + " size : " + DEFAULT_IMAGE_SIZE);
//				TODO get the image info from the saved file and fill these
				ImageMemMeta imageMemMeta = createImageMemMetaWithImageKey(userList.get(i), imageList.get(j),
						DEFAULT_IMAGE_SIZE, DEFAULT_CLIENT_BLOCK_SIZE, DEFAULT_HADOOP_BLOCK_SIZE, imageDir, dataFileList, currentFile);
				
				for (int k=0; k<dataFileList.size(); k++){
					if (dataFileList.get(k).contains("_temp")){
						System.out.println("			file name contains _temp : skipping");
						continue;
					}
					String dataFile = dataFileList.get(k);
					short dataFileNumber = Short.parseShort(dataFile);
					Path dataFilePath = new Path(imageDir + PATH_SEPERATOR + dataFile);
					System.out.println("			for data file " + dataFilePath.toString());

					long currPos = 12;
					FSDataInputStream dataFileStream = null;
					try {
						dataFileStream = fs.open(dataFilePath);
						int counter = 0;
						while(true){
							dataFileStream.seek(counter*unitSize);
							long addr = dataFileStream.readLong();
							dataFileStream.seek(counter*unitSize + 8);
							int seqNum = dataFileStream.readInt();
							dataFileStream.seek(counter*unitSize + 12);
//							System.out.println("		in while loop" + " " + addr + " " + seqNum + " " + counter);
							byte[] blockBuffer = new byte[(int)DEFAULT_CLIENT_BLOCK_SIZE];
							int totalRead = 0;
							while(totalRead<DEFAULT_CLIENT_BLOCK_SIZE){
								int currRead = dataFileStream.read(totalRead, blockBuffer, totalRead, (int)DEFAULT_CLIENT_BLOCK_SIZE - totalRead);
								totalRead += currRead;
							}
							AddrDataMapValue existingValue = imageMemMeta.blocksDirectory.get(addr);
							if (existingValue==null || existingValue.seqNumber<seqNum){
//								System.out.println("		adding " + addr + " to " + dataFileNumber + " " + seqNum + " " + currPos);
								imageMemMeta.blocksDirectory.put(addr, new AddrDataMapValue(dataFileNumber, seqNum, currPos));
							}
							currPos += unitSize;
							counter++;
						}
					} catch (Exception e) {
						System.out.println("exiting loop because of end of file");
						if (dataFileStream != null){
							try{
								dataFileStream.close();								
							} catch (Exception e1){
								e1.printStackTrace();
							}
						}
					}
				}
			}
		}
	}
	
	public HBSv5 createNewBlockStore(){
		return new HBSv5(this);
	}
	
	public HBSv5 getBlockStore(String imageKey){
		ImageMemMeta imageMemMeta = imagesDirectory.get(imageKey);
//		printImageMemMeta(imageMemMeta);
		return new HBSv5(imageMemMeta,this);
	}
	
	public ImageMemMeta getImageMemMeta(String imageKey){
		return imagesDirectory.get(imageKey);
	}
	
	public ImageMemMeta createImageMemMeta(String user, long imageSize, long clientBlockSize, long hadoopBlockSize){
		String imageKey = util.generateString();
		ImageMemMeta newImageMeta = new ImageMemMeta(imageKey, user, imageSize, (short)6, imageSize, clientBlockSize, hadoopBlockSize, this, false, null, null, (short)0);
		imagesDirectory.put(imageKey, newImageMeta);
		return newImageMeta;
	}
	
	public ImageMemMeta createImageMemMetaWithImageKey(String user, String imageKey, long imageSize, long clientBlockSize,
			long hadoopBlockSize, String imageDir, ArrayList<String> dataFileList, short currentFile){
		ImageMemMeta newImageMeta = new ImageMemMeta(imageKey, user, imageSize, (short)6, imageSize, clientBlockSize, hadoopBlockSize, this, true, imageDir, dataFileList, currentFile);
		imagesDirectory.put(imageKey, newImageMeta);		
		return newImageMeta;
	}
	
	public void printImagesDirectory(){
		for (Map.Entry<String, ImageMemMeta> entry:imagesDirectory.entrySet()){
			System.out.println(" image : " + entry.getKey());
			ImageMemMeta i = entry.getValue();
			printImageMemMeta(i);
		}
	}
	
	private void printImageMemMeta(ImageMemMeta i){
		for (Map.Entry<Long, AddrDataMapValue> entry2: i.blocksDirectory.entrySet()){
			System.out.println();
			System.out.print("		" + entry2.getKey() + " : ");
			System.out.print(entry2.getValue().addrOffset + " " + entry2.getValue().fileNumber + " "+ entry2.getValue().seqNumber);
		}

	}
	
}

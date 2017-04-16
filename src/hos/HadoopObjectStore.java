package hos;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletInputStream;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.ProtocolSignature;
import hbs.HbsUtil;
import restCalls.RestUtil;
import hbase.DBStrings;
import hbase.HbaseManager;
import hbase.HbaseUtil;

public class HadoopObjectStore implements ObjectProtocol {

	public HbaseManager hbaseManager = null;
	public HbsUtil util = new HbsUtil();
	public HbaseUtil hbaseUtil = new HbaseUtil();
	public String message;
	private static HadoopObjectStore defaultObjectStore = null;
	public static HadoopObjectStore getHadoopObjectStore(){
		if ( defaultObjectStore == null){
			defaultObjectStore = new HadoopObjectStore();
		}
		return defaultObjectStore;
	}
//	ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
	public HadoopObjectStore() {
		hbaseManager = new HbaseManager();
//		executor.scheduleAtFixedRate(BackGroundTasks.getBackGroundTasks(), 3, 3, TimeUnit.SECONDS);
	}
	
	@Override
	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> ListBuckets(UserId userID) {
		  // get entries with RestUtil.USER_NAME in userBuckets table and return 
		  HbaseUtil hbaseUtil = new HbaseUtil();
		 ArrayList<Result> res = hbaseUtil.getResultsForPrefix(DBStrings.Table_bucketsTableString, userID.id);
		 ArrayList<String> ret = new ArrayList<String>();

		 String colFam = DBStrings.DEFAULT_COLUMN_FAMILY;
		 String col1 = DBStrings.Col_bucketID;
		 
		 if (res == null) return ret;
		 
		 for (int i=0; i<res.size(); i++){
			 Result curr = res.get(i);
			 String rowKey = Bytes.toString(curr.getRow());
			 String bucketValue = Bytes.toString(curr.getValue(Bytes.toBytes(colFam), Bytes.toBytes(col1)));
			 ret.add(bucketValue);
		 }
		  return ret;	
	}

	private boolean folderAlreadyExists;
	
	@Override
	public String PutBucket(UserId userID, BucketInfo bucketID) {
		message = "";
		if (bucketID.bucketName.contains(",")){
			message = "Keys cannot contain ',' ";
			return message;
		}
		
		folderAlreadyExists = false;
		boolean success = createBucketInFolder(bucketID.bucketName);
		if (success){
	  		addEntriesToUserBuckets(bucketID);
	  		message = "Create Bucket is Successful";

	  	}
	  	else{
	  		message = "failed";
		  	if (folderAlreadyExists){
		  		message += ". Bucket with same name is already present";
		  	}
		  	else{
		  		message += ". Error creating bucket";
		  	}
	  	}
		return message;
	}


	@Override
	public String DeleteBucket(UserId userID, BucketId bucketID) {
		message = "";
/*
 * TODO - check for small files in the bucket
 * 	The deletebucketInFolder raises exception if there are files in it.
 * 	since fs.delete(_, false) is called.
 *  
 * 	If the files are smallFiles, they will be stored in the smallFiles folder.
 *  So, even if the folder is empty, the bucket may not be. Check in DB for this.
 *  HbaseUtil has scanWithPrefix. Use it to fix this.  
 * */
		boolean success = deleteBucketInFolder(bucketID.name);
	  	if (success){
	  		message += "success";
	  		deleteEntriesForDeleteBucket(bucketID.name);
	  	}
	  	else{
	  		message += "failed";
	  		if (noSuchBucket){
	  			message += ". No such bucket exists";
	  		}
	  		if (bucketNotEmpty){
	  			message += ". Bucket is not Empty";
	  		}
	  	}
		return message;
	}

	@Override
	public List<String> ListObjects(UserId userID, BucketId bucketID) {
		return getObjectList(userID.id + "," + bucketID.name + ",");
	}

	@Override
	public String PutObject(BucketId bucketID, ObjectInfo objectData, ServletInputStream reader) {
		
		if (objectData.fileName.contains(",")){
			return "failed. Key cannot contain ','";
		}
		
		FileSystem fs = util.getFileSystem();
		String responseMessage = "";
		String fileName =  objectData.fileName;
		String path;
		path = getNewFilePath(bucketID.name, fileName, isSmallFile(objectData.lenofFile));
		
		responseMessage += " len: " + objectData.lenofFile + " ";
		
		// For small files only: check for previous record and if it exists - add the previous entry to deleteTable
		
		Result r = HbaseUtil.getResult(DBStrings.Table_objectsTableString, DBStrings.USER_NAME + "," + bucketID.name + "," + objectData.fileName);
		if (!r.isEmpty()){
			String sizeOfFile = Bytes.toString(r.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_fileSize)));
			String cPath = Bytes.toString(r.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_url)));
			if (isSmallFile(Integer.parseInt(sizeOfFile))){
				// add entry to small files delete table
				int indexOfSlash = cPath.lastIndexOf('/');
				String corrHDFSPath = "/user/" + DBStrings.USER_NAME + "/smallFiles" + cPath.substring(indexOfSlash);
				
				hbaseManager.AddRowinTable(hbaseManager.smallFilesDeleteTable, corrHDFSPath, new String[]{"1"}, new String[]{cPath});
			}
			else{
				try {
					fs.delete(new Path(cPath), false);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		
		try{				
			
			Path p = new Path(path);
			objectData.path = path;
			
			if (fs.exists(p)){
				fs.delete(p, false);
			}
			
			FSDataOutputStream fsOut = fs.create(p);
			byte[] buffer = new byte[10240];
			for (int length = 0; (length = reader.read(buffer)) > 0;) {
	    			fsOut.write(buffer, 0, length);
    			}
			reader.close();
			
//			String line = null;
//			while((line=reader.readLine())!=null){
//				fsOut.writeBytes(line);
//				fsOut.writeBytes("\n");
//			}
			fsOut.close();
			
			responseMessage += "success";
			addEntriesForPutObject(bucketID, objectData);
		} catch (Exception e){
			responseMessage += ExceptionUtils.getStackTrace(e);
		}
		
		responseMessage = responseMessage + "path : "  + path + "\n";
		responseMessage = responseMessage + "name : "  + fileName + "\n";
		responseMessage = responseMessage + "encoding : "  + objectData.characterEnconding+ "\n";
		responseMessage = responseMessage + "contentType : "  + objectData.contentType+ "\n";
		
		// file upload is done
		
		
		
		return responseMessage;
	}

	@Override
	public boolean DeleteObject(BucketId bucketID, ObjectId objectKey) {
		message = "";
		FileSystem fs = util.getFileSystem();
		Result r = HbaseUtil.getResult(DBStrings.Table_objectsTableString, DBStrings.USER_NAME + "," + bucketID.name + "," + objectKey.id);
		if (!r.isEmpty()){
			String sizeOfFile = Bytes.toString(r.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_fileSize)));
			String cPath = Bytes.toString(r.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_url)));
			if (isSmallFile(Integer.parseInt(sizeOfFile))){
				// add entry to small files delete table
				int indexOfSlash = cPath.lastIndexOf('/');
				String corrHDFSPath = "/user/" + DBStrings.USER_NAME + "/smallFiles" + cPath.substring(indexOfSlash);
				hbaseManager.AddRowinTable(hbaseManager.smallFilesDeleteTable, corrHDFSPath, new String[]{"1"}, new String[]{cPath});
				
				if (cPath.startsWith("har:")){
					String harPath = cPath.substring(0, indexOfSlash);
					int indexOfHarSlash = harPath.lastIndexOf('/');
					String harName = harPath.substring(indexOfHarSlash);
					String harHDFS = "/user/" + DBStrings.USER_NAME + "/harFiles" + harName;
					Result r2 = HbaseUtil.getResult(DBStrings.Table_harFilesTableString, harHDFS);
					if (r2.isEmpty()){
						// no entry in har table??
						// something wrong
					}
					else{
						String usedSpace = Bytes.toString(r2.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_validSpace)));
						String diskSpace = Bytes.toString(r2.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_SpaceOnDisk)));
						
						long us= Long.parseLong(usedSpace);
						
						us -= Long.parseLong(sizeOfFile);
						usedSpace = String.valueOf(us);
						
						hbaseManager.AddRowinTable(hbaseManager.harFilesTable, harHDFS,
								new String[]{DBStrings.Col_validSpace, DBStrings.Col_SpaceOnDisk}, new String[]{usedSpace, diskSpace});
						
					}
				}
				
			}
			else{
				try {
					fs.delete(new Path(cPath), false);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			// delete entry from DB
		  	boolean part2Succ = deleteEntriesFromTable(bucketID.name, objectKey.id);
		  	return part2Succ;
		}
		else{
			return false;
		}
		
	}

	@Override
	public ObjectOut GetObject(BucketId bucketID, ObjectId objectKey) {
		message = "";
  		// get File path and its name
		ObjectInfoMetaData oimd =RestUtil.getObjectFromParams(bucketID.name, objectKey.id); 
	  	ObjectInfo obj = oimd.objectInfo;
	  	String fileName  = obj.fileName;
	  	
	  	FSDataInputStream fsin  = null;
	  	
	  	String objPath = obj.path;
	  	Path p = new Path("none");
	  	if (objPath.startsWith("har:")){
	  		// the file is in a har file
	  		int lastIndex = objPath.lastIndexOf('/');
	  		String harPath = objPath.substring(0, lastIndex);
	  		
	  		try {
				p = new Path(new URI(objPath));
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		  	HarFileSystem hfs = util.getHarFileSystem();
		  	try {
				hfs.initialize(new URI(harPath),util.getConfiguration());
			  	fsin = hfs.open(new Path(objPath)); 
		  	} catch (Exception e) {
				e.printStackTrace();
			}
	  	}
	  	else{
	  		p = new Path(objPath);
		  	FileSystem fs = util.getFileSystem();
		  	
		  	try {
				fsin =fs.open(p);
			} catch (IOException e) {
				e.printStackTrace();
			}
	  	}
	  	
	  	
		DataInputStream din = new DataInputStream(fsin);
	  	
	  	return (new ObjectOut(obj, din, oimd.metaData));
	}

	
	
	/*
	 * the above uses below 
	 * */
	
	  private ArrayList<String> getObjectList(String DBPrefix){
		  
		  // get entries with RestUtil.USER_NAME_bucketKey in objectsTable and return 

		  System.out.println(" starting getObjectList");
		  ArrayList<Result> result = hbaseUtil.getResultsForPrefix(DBStrings.Table_objectsTableString, DBPrefix);
		  System.out.println("got results");
		  ArrayList<String> ret = new ArrayList<String>();
		  if (result == null) return ret;
		  for (int i=0; i<result.size(); i++){
			  String curr = Bytes.toString(result.get(i).getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_ObjectID)));
			  ret.add(curr);
		  }
		  return ret;
	  }

	
	private String getNewFilePath(String bKey, String oKey, boolean isSmall){
		String prefix = "/user/" + DBStrings.USER_NAME + "/";
		String suffix = "_" + HbaseUtil.getJuilianTimeStamp();
		if (isSmall){
			return  prefix + ObjectProtocol.smallFilesFolderName   + "/" + bKey + ObjectProtocol.FILENAME_SEPERATOR + oKey + suffix;
		}
		return prefix +  bKey + "/" + oKey + suffix ;
	}
	
	private boolean isSmallFile(int n){
		long frac = ((long)n*100)/ObjectProtocol.HADOOP_BLOCK_SIZE;
		if (frac<50){
			return true;
		}
		return false;
	}
	
	private boolean addEntriesForPutObject(BucketId b, ObjectInfo o){
		// add into objectsTable
		String rowKey = DBStrings.USER_NAME + "," + b.name + "," + o.fileName;
		String colNames[] = new String[DBStrings.num_metadata];
		colNames[0] = DBStrings.Col_url;
		colNames[1] = DBStrings.Col_charEncoding;
		colNames[2] = DBStrings.Col_contentType;
		colNames[3] = DBStrings.Col_fileSize;
		colNames[4] = DBStrings.Col_userMetdata;
		colNames[5] = DBStrings.Col_ObjectID;
		colNames[6] = DBStrings.Col_timeStamp;

		StringBuffer sb = new StringBuffer();
		for (Entry<String,String[]> entry : o.uMetadata.entrySet()) {
			    sb.append(entry.getKey() + ":" + entry.getValue()[0] + "\n");
		}

		
		String colValues[] = new String[DBStrings.num_metadata];
		colValues[0] = o.path; 
		colValues[1] = o.characterEnconding;
		colValues[2] = o.contentType;
		colValues[3] = String.valueOf(o.lenofFile);
		colValues[4] = sb.toString();
		colValues[5] = o.fileName;
		colValues[6] = String.valueOf(System.currentTimeMillis());
		
		for (int i=0; i<DBStrings.num_metadata; i++){
			if (colValues[i] == null) colValues[i] = "";
		}
		
		hbaseManager.AddRowinTable(hbaseManager.objectsTable, rowKey, colNames, colValues);
		return true;
	}

	
	
	private boolean deleteEntriesFromTable(String bucketKey, String objectKey){
		String rowKey = DBStrings.USER_NAME + "," + bucketKey + "," + objectKey;
		hbaseManager.DeleteRowinTable(hbaseManager.objectsTable, rowKey);
		return true;
	}
  
	private boolean deleteObjectInBucket(String objPath){
	  	if (objPath.startsWith("har:")){
	  		return true;
	  	}
	  	else{
	  		FileSystem fs = util.getFileSystem();
	  		try {
				fs.delete(new Path(objPath), false);
				return true;
			} catch (Exception e) {
				e.printStackTrace();
			}
	  		return false;
	  	}
	}

	public void errorlog(String content)
	{
		try {
			File file = new File("webapps/log.txt");
			content += "\n";
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(content);
			bw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public boolean createBucketInFolder(String bucketKey){
		  folderAlreadyExists = false;
		  errorlog("hi!");
		  FileSystem fs = util.getFileSystem();
		  errorlog("hi!");
		  errorlog("/user/" + DBStrings.USER_NAME + "/" + bucketKey);
		  Path newPath = new Path("/user/" + DBStrings.USER_NAME + "/" + bucketKey);
		  errorlog("hi! end");

		  try {
			  errorlog("hi!- start");

			if (fs.exists(newPath)){
				  errorlog("hi!-path initial");
				  folderAlreadyExists = true; 
				  return false;
			}
			else{
				  errorlog("hi!-mk initial");

				fs.mkdirs(newPath);
				  errorlog("hi!-mk final");

				return true;
			}
		} catch (Exception e) {
			  errorlog("hi - exception");

			//			message += Exception
			e.printStackTrace();
		}
		  return false;
	  }
	  
	  public void addEntriesToUserBuckets(BucketInfo bucketKey){
		  
		  hbaseManager.AddRowinTable(hbaseManager.bucketsTable, bucketKey.ownerUser + "," + bucketKey.bucketName, 
				  new String[]{DBStrings.Col_bucketID} , new String[]{bucketKey.bucketName});
	  }

	  
	  boolean noSuchBucket, bucketNotEmpty;

//	 TODO - change to include search for small files 
	  public boolean deleteBucketInFolder(String bucketKey){
		  FileSystem fs = util.getFileSystem();
		  noSuchBucket = true;
		  bucketNotEmpty  = false;
		  Path p = new Path(util.genDefaultUserPath(bucketKey));
		  try {
				if (fs.exists(p)){
					noSuchBucket = false;
//					message += "bucket exists";
					try{
						fs.delete(p, false); 						
						return true;
					} catch (Exception e){
//						message += "exc23 ";
						bucketNotEmpty = true;
					}
					return false;
				}
//				else{
//					message += " bucket not exists";
//				}
			} catch (Exception e) {
//				message += "exc";
				e.printStackTrace();
			}
		  return false;
	  }

	  private void deleteEntriesForDeleteBucket(String bucketId){
		  hbaseManager.DeleteRowinTable(hbaseManager.bucketsTable, DBStrings.USER_NAME + "," + bucketId);
	  }
	
}

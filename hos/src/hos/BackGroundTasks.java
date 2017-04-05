package hos;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import hbase.DBStrings;
import hbase.HbaseManager;
import hbase.HbaseUtil;
import hbs.HbsUtil;
import java.security.SecureRandom;
import java.math.BigInteger;
import java.net.URI;


public class BackGroundTasks implements Runnable{

	HbaseManager hbaseManager = HadoopObjectStore.getHadoopObjectStore().hbaseManager;
	private int taskNo = -1;
	
	public static int ARCHIVE_TASK = 0;
	public static int UNARCHIVE_TASK = 1;
	public static int CLEANUP_TASK = 2;
	
	HbsUtil util = new HbsUtil();
	FileSystem fs = util.getFileSystem();
	HarManager hm = new HarManager();

	public BackGroundTasks(int taskNo){
		this.taskNo = taskNo;
	}
	
	public String getSmallFilesDir(){
		return "/user/" + DBStrings.USER_NAME + "/smallFiles";
	}
	
	public long getSmallFilesSize(){
		String smallFilesDir = getSmallFilesDir();
		long ret = -1;
		try {
			ret = fs.getContentSummary(new Path(smallFilesDir)).getLength();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}

	public ArrayList<String> getFilesList(){
		ArrayList<String> ret = new ArrayList<String>();
		try {
			FileStatus[] status = fs.listStatus(new Path(getSmallFilesDir()));
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

	public ArrayList<Long> getSizesList(){
		ArrayList<Long> ret = new ArrayList<Long>();
		try {
			FileStatus[] status = fs.listStatus(new Path(getSmallFilesDir()));
			for (int i=0;i<status.length;i++){
				ret.add(status[i].getLen());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}

	
	public ArrayList<byte[]> getTimeStampList(ArrayList<String> fileNames){
		int nFiles = fileNames.size();
		ArrayList<byte[]> ret = new ArrayList<byte[]>();
		for (int i=0; i<nFiles; i++){
			String fileName = fileNames.get(i);
			int indexOfUnder = fileName.lastIndexOf('_');
			String rowKey = DBStrings.USER_NAME + ","  + fileNames.get(i).substring(0, indexOfUnder);
			System.out.println("row : " + rowKey);
			Result r = HbaseUtil.getResult(DBStrings.Table_objectsTableString, rowKey);
			ret.add(r.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_timeStamp)));
		}
		return ret;
	}
	
	public String genHarFileName(){
		SecureRandom random = new SecureRandom();
		String newHarName = new BigInteger(130, random).toString(32) + ".har";
		try {
			if (fs.exists(new Path("/user/" + DBStrings.USER_NAME + "/harFiles/" + newHarName))){
				return genHarFileName();
			}
			else{
				return newHarName;
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return genHarFileName();
	}
	
	public void archive(){
		long smallFilesSize = getSmallFilesSize();
//		TODO change this to greater than 
		if (smallFilesSize>ObjectProtocol.ARCHIVE_SIZE_THRESHOLD){
			ArrayList<String> files = getFilesList();
			if (files.size()<2) return;
			ArrayList<Long> sizes = getSizesList();
			String harFileName = genHarFileName();
			String harDirPath = "/user/" + DBStrings.USER_NAME + "/harFiles" ;
			String harPath = harDirPath + "/" + harFileName;
			hbaseManager.AddRowinTable(hbaseManager.harFilesTable, harPath, 
					new String[]{DBStrings.Col_validSpace, DBStrings.Col_SpaceOnDisk}, new String[]{String.valueOf(smallFilesSize), String.valueOf(smallFilesSize)});
			ArrayList<byte[]> timeStampList = getTimeStampList(files);
			
			hm.makeArchive(util.FSNAME + getSmallFilesDir(), files, util.FSNAME + harDirPath, harFileName);
			
			
			// update DB 
			for (int i=0; i<files.size(); i++){	
				HTable table = null;
				try{
					table = new HTable(HbaseUtil.con, DBStrings.Table_objectsTableString);

					String fileName = files.get(i);
					String entryName = removeUnderScore(fileName);
					String rowKey = DBStrings.USER_NAME + "," + entryName;
					System.out.println("rowKey : " + rowKey);
					Put p = new Put(Bytes.toBytes(rowKey));
					p.addColumn(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_url), 
							Bytes.toBytes("har:///user/" + DBStrings.USER_NAME + "/harFiles/" + harFileName + "/" + fileName));
					p.addColumn(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_timeStamp), 
							Bytes.toBytes(String.valueOf(System.currentTimeMillis() ) ) );
					boolean flag = table.checkAndPut(Bytes.toBytes(rowKey), Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_timeStamp),
							timeStampList.get(i), p);
					System.out.println("entryName : " + entryName + " time: " + Bytes.toString(timeStampList.get(i)));
					
					if (!flag){
						
						System.out.println("differnet timestamps");
						
						// put has failed
						
						// either No Row or timeStamps are different
						// add this file to deleteQueue
						String corrHDFS = "/user/" + DBStrings.USER_NAME  + "/smallFiles/"+ fileName;
						hbaseManager.AddRowinTable(hbaseManager.smallFilesDeleteTable, corrHDFS, new String[]{"1"}, new String[]{corrHDFS});
						
						// update space in harfilesTable
						String harHDFS = "/user/" + DBStrings.USER_NAME + "/harFiles" + harFileName;
						Result r2 = HbaseUtil.getResult(DBStrings.Table_harFilesTableString, harHDFS);
						String usedSpace = Bytes.toString(r2.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_validSpace)));
						String diskSpace = Bytes.toString(r2.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_SpaceOnDisk)));
						
						System.out.println("usedSpace : " + usedSpace);
						System.out.println("diskSpace : " + diskSpace);
						long us= Long.parseLong(usedSpace);
						
						us -= sizes.get(i);
						usedSpace = String.valueOf(us);
						
						hbaseManager.AddRowinTable(hbaseManager.harFilesTable, harHDFS,
								new String[]{DBStrings.Col_validSpace, DBStrings.Col_SpaceOnDisk}, new String[]{usedSpace, diskSpace});
					
					}
					table.close();
				} catch (Exception e){
					if (table!=null){
						try {
							table.close();
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
					}
					e.printStackTrace();
				}
			}
			
			// delete files
			for (int i=0; i<files.size(); i++){
				String fileName = files.get(i);
				String path = "/user/" + DBStrings.USER_NAME + "/smallFiles/" + fileName;
				try {
					fs.delete(new Path(path), false);
				} catch (IllegalArgumentException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}
	}

	public String removeUnderScore(String a){
		int index = a.lastIndexOf('_');
		return a.substring(0, index);
	}
	
	
	
	
	
	/**
	 * 
	 * @param harHDFSURI
	 * 			"/user/hduser/harFiles/harName.har"
	 * @return
	 */
	public ArrayList<String> getListOfFilesInHar(String harHDFSURI){
		HbsUtil util = new HbsUtil();
		HarFileSystem hfs = util.getHarFileSystem();
		ArrayList<String> ret = new ArrayList<String>();
		try {
			hfs.initialize(new URI(harHDFSURI), util.getConfiguration());
			FileStatus[] fsArray  = hfs.listStatus(new Path(harHDFSURI));
			for (int i=0; i<fsArray.length; i++){
				String filePath = fsArray[i].getPath().toString();
				int index = filePath.lastIndexOf('/');
				ret.add(filePath.substring(index+1));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}

//	TODO pick har file
	public String pickHar(){
		ArrayList<Result> harList = HbaseUtil.getAllEntries(DBStrings.Table_harFilesTableString);
		for (int i=0; i<harList.size(); i++){
			Result result = harList.get(i);
			long validSpace = Long.parseLong(Bytes.toString(result.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_validSpace))));
			long spaceDisk = Long.parseLong(Bytes.toString(result.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_SpaceOnDisk))));
			double fraction = (validSpace*100.0)/((double)spaceDisk);
			System.out.println("valid: "+ validSpace + " disk : " + spaceDisk + " fraction : " + fraction);
			if (fraction < ObjectProtocol.UNARCHIVE_PERCENT_THRESHOLD){
				return  Bytes.toString(result.getRow());
			}
		}
		return null;
	}
	
	public void unarchive(){
		String chosenHarPath =  pickHar();
		if (chosenHarPath == null) return;
		ArrayList<String> files = getListOfFilesInHar(chosenHarPath);
		
		for (int i=0; i<files.size();i++){
			System.out.println(files.get(i));
		}
		
		
		ArrayList<byte[]> timeStampList = getTimeStampList(files);
		
		for (int i=0; i<timeStampList.size(); i++){
			System.out.println(timeStampList.get(i));
		}
		
		hm.unArchiveAll(chosenHarPath, "/user/" + DBStrings.USER_NAME + "/smallFiles");

		HTable table = null;
		try {
			table = new HTable(HbaseUtil.con, DBStrings.Table_objectsTableString);
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		// update DB 
		for (int i=0; i<files.size(); i++){	
			try{
				String fileName = files.get(i);
				String entryName = removeUnderScore(fileName);
				String rowKey = DBStrings.USER_NAME + "," + entryName;
				System.out.println("rowKey : " + rowKey + " entryKey :" + entryName);
				Put p = new Put(Bytes.toBytes(rowKey));
				p.addColumn(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_url), 
						Bytes.toBytes("/user/" + DBStrings.USER_NAME + "/smallFiles/" + fileName));
				p.addColumn(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_timeStamp), 
						Bytes.toBytes(String.valueOf(System.currentTimeMillis() ) ) );
				boolean flag = table.checkAndPut(Bytes.toBytes(rowKey), Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_timeStamp),
						timeStampList.get(i), p);
				System.out.println("entryName : " + entryName + " time: " + Bytes.toString(timeStampList.get(i)));
				
				if (!flag){
					
					System.out.println("differnet timestamps");
					
					// put has failed
					
					// either No Row or timeStamps are different
					// add this file to deleteQueue
					String corrHDFS = "/user/" + DBStrings.USER_NAME  + "/smallFiles/"+ fileName;
					hbaseManager.AddRowinTable(hbaseManager.smallFilesDeleteTable, corrHDFS, new String[]{"1"}, new String[]{corrHDFS});
					
				}
				
			} catch (Exception e){
				e.printStackTrace();
			}
		}

		// delete entry in harfilesTable
		hbaseManager.DeleteRowinTable(hbaseManager.harFilesTable, chosenHarPath);
		// delete harFile
		try {
			fs.delete(new Path(chosenHarPath), false);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			table.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	
	}
	
	public void cleanUp(){
		ArrayList<String> deleteFiles = HbaseUtil.getRowKeys(DBStrings.Table_smallFilesDeleteTableString);
		for (int i=0; i<deleteFiles.size(); i++){
			String deleteFilePath = deleteFiles.get(i);
			Path p = new Path(deleteFilePath);
			try {
				if (fs.exists(p)){
					fs.delete(p, true);
					hbaseManager.DeleteRowinTable(hbaseManager.smallFilesDeleteTable, deleteFilePath);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("clean up complete");
	}
	
	@Override
	public void run() {
		System.out.println("starting task - " + taskNo);
		if (taskNo == ARCHIVE_TASK){
			System.out.println("executing archive");
			archive();
		}
		else if (taskNo == UNARCHIVE_TASK){
			System.out.println("executing unarchive");
			unarchive();
		}
		else if (taskNo == CLEANUP_TASK){
			System.out.println("executing clean up");
			cleanUp();
		}
		System.out.println("task done - " + taskNo);
	}
}

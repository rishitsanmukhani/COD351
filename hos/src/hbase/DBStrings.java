package hbase;

public final class DBStrings {
	private DBStrings(){
		
	}
	
	public static final String USER_NAME = "baadalvm";
	
	public final static String DEFAULT_COLUMN_FAMILY = "DEFAULT_CF";
	public final static char DELIMITER = ',';
	
	//Table1
	public final static String Table_bucketsTableString = "bucketstable";
	//KeyCol - userid,bucketid
	public final static String KeyCol_bucketksList = "USERSBUCKET";
	//Col - userid
	public final static String Col_bucketID = "BUCKETID";
	
	
	//Table2
	public final static String Table_smallFilesDeleteTableString = "smallfilesdeletetable";
	//KeyCol - URL 
	public final static String Col_FilePath = "FilePath";
	
	//Table3
	public final static String Table_harFilesTableString =  "harfilestable";
	//KeyCol - harFileurl 
	public final static String KeyCol_harFileUrl = "HARFILEURL";
	//Col - Validspace
	public final static String Col_validSpace = "VALIDSPACE";
	//Col - SpaceOnDisk
	public final static String Col_SpaceOnDisk = "SPACEONDISK";
	
	//Table4
	public final static String Table_objectsTableString = "objectstable";
	//KeyCol 
	public final static String KeyCol_ObjectsList = "OBJECTSLIST";
	//Col
	public final static String Col_url = "URL";
	//Col
	public final static String Col_charEncoding = "CHARENCODING";
	//Col
	public final static String Col_contentType = "CONTENTTYPE";
	//Col
	public final static String Col_fileSize = "FILESIZE";
	//Col
	public final static String Col_userMetdata = "USERMETADATA";
	//Col
	public final static String Col_ObjectID = "OBJECTID";
	//Col
	public final static String Col_timeStamp = "TIMESTAMP";
	
	public final static int num_metadata = 7;
}

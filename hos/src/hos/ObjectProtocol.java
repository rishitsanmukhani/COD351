package hos;


import java.io.BufferedReader;
import java.io.DataInputStream;
import java.util.List;

import javax.servlet.ServletInputStream;

import org.apache.hadoop.ipc.VersionedProtocol;


public interface ObjectProtocol extends VersionedProtocol {

	public static final String smallFilesFolderName = "smallFiles";
	public static final long HADOOP_BLOCK_SIZE = 128<<20;
	public static final String FILENAME_SEPERATOR = ",";
	public static final String USER_BUCKETS_TABLE_NAME = "emp";
	public static final String USER_BUCKETS_TABLE_COL_FAM = "pData";
	public static final String USER_BUCKETS_TABLE_COL_1 = "name";
	public static final String USER_BUCKETS_TABLE_COL_2 = "nwe";
	public static final long ARCHIVE_SIZE_THRESHOLD = 500<<20;
	public static final double UNARCHIVE_PERCENT_THRESHOLD = 50.0;
	
	public static final long versionID = 1L;
	
	public List<String> ListBuckets(UserId userID);
	
	public String PutBucket(UserId userID, BucketInfo bucketID);
		
	public String DeleteBucket(UserId userID, BucketId bucketID);
	
	// don't know the metadata class type
	public void GetBucket(UserId userID, BucketId bucketID);
	
	public void UpdateBucket(UserId userID, BucketId bucketID, );
	// edited
	
	public List<String> ListObjects(UserId userID, BucketId bucketID);
	
	public String PutObject(BucketId bucketID, ObjectInfo objectInfo, ServletInputStream reader);
	
	public boolean DeleteObject(BucketId bucketID, ObjectId objectKey);
	
	public ObjectOut GetObject(BucketId bucketID,ObjectId objectKey);
	
	//
	public boolean ComposeObjects(UserId userID, BucketId bucketID, List<String> listObjects);
	
	public boolean CopyObject(BucketId bucketID, ObjectId sourceObject, ObjectId destinationObject);
	
	public List<String> ListObjects(UserId userID, BucketId bucketID, int filter);
	
	public boolean UpdateObject(BucketId bucketID, ObjectId objectKey, int metaData);
	
	public boolean UpdateMetaData(BucketId bucketID, ObjectId objectKey, int metaData);
	
	public void Watchall();
	
	//
	
}

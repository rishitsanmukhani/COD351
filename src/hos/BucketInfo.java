package hos;

public class BucketInfo {
	String  bucketName = null;
	String ownerUser = null;
	
	public BucketInfo(String owner, String bName){
		this.ownerUser = owner;
		this.bucketName = bName;
	}
	
}

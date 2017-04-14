package restCalls;

import java.util.HashMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import hbase.DBStrings;
import hbase.HbaseUtil;
import hos.ObjectInfo;
import hos.ObjectInfoMetaData;

public class RestUtil {

	public static final String USER_NAME = "hduser";
	
	public static String getPathFromParams(String bucketKey, String objectKey){;
		return "/user/" + DBStrings.USER_NAME + "/" + bucketKey + "/" + objectKey;				
	}
	
	public static ObjectInfoMetaData getObjectFromParams(String bucketKey, String objectKey){
		ObjectInfo ret = new ObjectInfo("", "", "", "", 0, new HashMap<String, String[]>());
		
		HbaseUtil hbaseUtil  = new HbaseUtil();
		String rowKey = DBStrings.USER_NAME + "," + bucketKey + ","  + objectKey;
		Result result = hbaseUtil.getResult(DBStrings.Table_objectsTableString, rowKey);
		
		ret.path = Bytes.toString(result.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_url)));
		ret.characterEnconding = Bytes.toString(result.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_charEncoding)));
		ret.contentType= Bytes.toString(result.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_contentType)));
		String lenString = Bytes.toString(result.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_fileSize)));
		ret.lenofFile = Integer.parseInt(lenString);
		
		ret.fileName = Bytes.toString(result.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_ObjectID)));
		String metaDataString = Bytes.toString(result.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_userMetdata)));
		
		return new ObjectInfoMetaData(ret, metaDataString);
	}
	
	
	public void readTest(){
		HbaseUtil h = new HbaseUtil();
		Result result = h.getResult("emp", "1");
		byte [] value = result.getValue(Bytes.toBytes("pData"),Bytes.toBytes("name"));

		byte [] value1 = result.getValue(Bytes.toBytes("pData"),Bytes.toBytes("nwe"));

	      // Printing the values
	      String name = Bytes.toString(value);
	      String city = Bytes.toString(value1);
	      
	      System.out.println("name: " + name + " nwe: " + city);

	}
	
}

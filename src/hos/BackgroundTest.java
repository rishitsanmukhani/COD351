package hos;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import hbase.DBStrings;
import hbase.HbaseUtil;
import hbs.HbsUtil;

public class BackgroundTest {
	public static void main(String args[]){
		BackGroundTasks b = new BackGroundTasks(0);
//		listFilesInHar();
//		b.unarchive();
//		System.out.println( b.pickHar());
//		b.archive();
//		b.cleanup();
		
//		b.getSmallFilesDir();
		
//		checkAndPutTest();
	}
	
	public static void checkAndPutTest(){
		String rowKey = "hduser,iitd,a.txt";
		Put p = new Put(Bytes.toBytes(rowKey));
		p.addColumn(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_url), 
				Bytes.toBytes("har:///user/" + DBStrings.USER_NAME + "/harFiles/" + "foo.har" + "/" + "a.tst"));
		p.addColumn(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_timeStamp), 
				Bytes.toBytes(String.valueOf(System.currentTimeMillis()) ));
		HTable table = null;
		try{
			table = new HTable(HbaseUtil.con, DBStrings.Table_objectsTableString);
			boolean flag = table.checkAndPut(Bytes.toBytes(rowKey), Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_timeStamp),
					Bytes.toBytes("14637013582652"), p);

			System.out.println(flag);
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
	public static void listFilesInHar(){
		HbsUtil util = new HbsUtil();
		HarFileSystem hfs = util.getHarFileSystem();
		try {
			hfs.initialize(new URI("/user/hduser/harFiles/7al0kfdjem1pmecousthhmg1t0.har"), util.getConfiguration());
			FileStatus[] fsArray  = hfs.listStatus(new Path("/user/hduser/harFiles/7al0kfdjem1pmecousthhmg1t0.har"));
			for (int i=0; i<fsArray.length; i++){
				System.out.println(fsArray[i].getPath().toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}

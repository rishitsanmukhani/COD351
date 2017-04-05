package hbase;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseUtil {
	
	public static Configuration con = HBaseConfiguration.create();
	
	public static Result getResult(String tableName ,String rowKey){
		HTable table = null;
		try{
			table = new HTable(con, tableName);
			Get g = new Get(Bytes.toBytes(rowKey));			
			Result result = table.get(g);
			table.close();
			return result;
		} catch (Exception e){
			try {
				if (table!=null) table.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		}
		return null;
	}

	public static ArrayList<Result> getAllEntries(String tableName){
		HTable table = null;
		ArrayList<Result> ret = new ArrayList<Result>();
		try{
			table = new HTable(con, tableName);
			Scan scan = new Scan();
			ResultScanner scanner = table.getScanner(scan);
			for (Result result = scanner.next(); (result != null); result = scanner.next()) {
				ret.add(result);
			}
		} catch (Exception e){
			if (table != null){
				try{
					table.close();
				} catch (Exception e1){
					e1.printStackTrace();
				}
			}
			e.printStackTrace();
		}
		return ret;
	}

	public static ArrayList<String> getRowKeys(String tableName){
		ArrayList<Result> res = getAllEntries(tableName);
		ArrayList<String> ret = new ArrayList<String>();
		for (int i=0; i<res.size(); i++){
			Result result = res.get(i);
			ret.add(Bytes.toString(result.getRow()));
		}
		return ret;
	}
	
	public ArrayList<Result> getResultsForPrefix(String tableName, String prefixString){
		ArrayList<Result> ret = new ArrayList<Result>();
		try {
			HTable table = new HTable(con, tableName);
			byte[] prefix = Bytes.toBytes(prefixString);
			Scan scan = new Scan(prefix);
			Filter prefixFilter = new PrefixFilter(prefix);
			scan.setFilter(prefixFilter);
			ResultScanner resultScanner = table.getScanner(scan);
			for (Iterator<Result> iterator = resultScanner.iterator(); iterator.hasNext();) {
			        ret.add(iterator.next());
			}
			return ret;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public static long getJuilianTimeStamp(){
		return System.currentTimeMillis();
	}
	
	public static long getTimeStampFromRow(String tableName, String rowKey){
		Result r = getResult(tableName, rowKey);
		// gets time stamp from URL - 6th in alphabetical order. => index = 5
		return (r.raw()[5].getTimestamp());
	}
	
	public void printResult(Result result) {
	    String returnString = "";
	    returnString += Bytes.toString(result.getRow()) + " : ";
	    returnString += Bytes.toString(result.getValue(Bytes.toBytes("pData"), Bytes.toBytes("name"))) + ", ";
	    returnString += Bytes.toString(result.getValue(Bytes.toBytes("pData"), Bytes.toBytes("nwe")));
	    System.out.println(returnString);
	}

	
}

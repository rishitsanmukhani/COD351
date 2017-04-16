package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.jruby.exceptions.RaiseException;

public class HbaseManager {
	Configuration conf ;
	Connection conn;
	Admin admin ;
	
	public static final String DEFAULT_USER_NAMe = "rishit";
	
	public HTableDescriptor bucketsTable;
	public HTableDescriptor smallFilesDeleteTable;
	public HTableDescriptor harFilesTable;
	public HTableDescriptor objectsTable;
	
	public HbaseManager(){
		conf = HBaseConfiguration.create();
//		conf.clear();
//		conf.set("hbase.zookeeper.quorum", "127.0.0.1");
//		conf.set("hbase.zookeeper.property.clientPort","2181");
		try {
			conn = ConnectionFactory.createConnection(conf);
			admin = conn.getAdmin();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		SetInternalTablesSchema();
		CheckandCreateInternalTables();
	}
	
	private void SetInternalTablesSchema() {
		
		HColumnDescriptor defaultFam = new HColumnDescriptor(DBStrings.DEFAULT_COLUMN_FAMILY);
		
		bucketsTable = new HTableDescriptor(TableName.valueOf(DBStrings.Table_bucketsTableString));
		bucketsTable.addFamily(defaultFam);
		
		smallFilesDeleteTable = new HTableDescriptor(TableName.valueOf(DBStrings.Table_smallFilesDeleteTableString));
		smallFilesDeleteTable.addFamily(defaultFam);
		
		harFilesTable = new HTableDescriptor(TableName.valueOf(DBStrings.Table_harFilesTableString));
		harFilesTable.addFamily(defaultFam);
		
		objectsTable = new HTableDescriptor(TableName.valueOf(DBStrings.Table_objectsTableString));
		objectsTable.addFamily(defaultFam);
		
	}
	
	public void CheckandCreateInternalTables(){
		
		try {
			if(!admin.tableExists(bucketsTable.getTableName())){
				CreateTable(bucketsTable);
			}
			if(!admin.tableExists(smallFilesDeleteTable.getTableName())){
				CreateTable(smallFilesDeleteTable);
			}
			if(!admin.tableExists(harFilesTable.getTableName())){
				CreateTable(harFilesTable);
			}
			if(!admin.tableExists(objectsTable.getTableName())){
				CreateTable(objectsTable);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void DeleteRowinTable(HTableDescriptor tabledescriptor, String Key){
		Delete d = new Delete(Bytes.toBytes(Key));
		try {
			Table table = conn.getTable(tabledescriptor.getTableName());
			table.delete(d);
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public void CreateTable(HTableDescriptor table){
		try {
			if(admin.tableExists(table.getTableName())){
				admin.disableTable(table.getTableName());
			    admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void AddRowinTable(HTableDescriptor tabledescriptor, String Key, String[] colnames, String[] colvalues){
		if (colnames.length != colvalues.length ){
			System.err.println("Incorrect add schema, colkeys != colvalues");
			return;
		}		
		if(tabledescriptor.getNameAsString().equals(DBStrings.Table_bucketsTableString)){
//			if(colnames.length != 1 || colnames[0] != DBStrings.Col_userID){
//				System.err.println("Incorrect add schema, bucketsTable");
//				return;
//			}
		}
		else if(tabledescriptor.getNameAsString().equals(DBStrings.Table_harFilesTableString)){
//			if(colnames.length != 0){
//				System.err.println("Incorrect add schema, harFilesTable");
//				return;
//			}
		}
		else if(tabledescriptor.getNameAsString().equals(DBStrings.Table_objectsTableString)){
//			if(colnames.length != DBStrings.num_metadata ){
//				System.err.println("Incorrect add schema, ObjectsTable");
//				return;
//			}
		}
		else if(tabledescriptor.getNameAsString().equals(DBStrings.Table_smallFilesDeleteTableString)){
//			if(colnames.length != 0 ){
//				System.err.println("Incorrect add schema, smallFilesDeleteTable");
//				return;
//			}
		}
		else{
			System.err.println("Incorrect add schema, no table ");
			return;
		}
		Put p = new Put(Bytes.toBytes(Key));
		for(int i=0; i < colnames.length ; i++){
			p.addColumn(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(colnames[i]), Bytes.toBytes(colvalues[i]));	
		}
		try {
			Table table = conn.getTable(tabledescriptor.getTableName());
			table.put(p);
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}

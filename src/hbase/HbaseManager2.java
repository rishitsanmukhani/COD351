package hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.jruby.exceptions.RaiseException;

public class HbaseManager2 {
	Configuration conf ;
	Connection conn;
	Admin admin ;
	
	public static final String DEFAULT_USER_NAME = "rishit";
	
	public HTableDescriptor dataTable;
	public HTableDescriptor internalsTable;
	public int TotalNumRegions = 9;
	
	public HbaseManager2(){
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
		
		dataTable = new HTableDescriptor(TableName.valueOf(DBStrings.Table_hbs_data_table));
		dataTable.addFamily(defaultFam);

		internalsTable = new HTableDescriptor(TableName.valueOf(DBStrings.Table_hbs_internals_table));
		internalsTable.addFamily(defaultFam);

	}
	
	
	public void CheckandCreateInternalTables(){
		try {
			if(!admin.tableExists(dataTable.getTableName())){
				CreateTable(dataTable, TotalNumRegions);
			}
			if(!admin.tableExists(internalsTable.getTableName())){
				CreateTable(internalsTable, 1);
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
	
	
	public void CreateTable(HTableDescriptor table, int TotalRegions){
		try {
			if(admin.tableExists(table.getTableName())){
				admin.disableTable(table.getTableName());
			    admin.deleteTable(table.getTableName());
			}
			byte[][] splits = new RegionSplitter.HexStringSplit().split(
					TotalRegions);
			admin.createTable(table,splits);
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void AddRowinTable(HTableDescriptor tabledescriptor, String Key, String colname, byte[] value){
		Put p = new Put(Bytes.toBytes(Key));
		p.addColumn(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(colname), value );	
		try {
			Table table = conn.getTable(tabledescriptor.getTableName());
			table.put(p);
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void AddRowinTable(HTableDescriptor tabledescriptor, String Key, String colname, String value){
		Put p = new Put(Bytes.toBytes(Key));
		p.addColumn(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(colname), Bytes.toBytes(value) );	
		try {
			Table table = conn.getTable(tabledescriptor.getTableName());
			table.put(p);
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void AddRowinTable(HTableDescriptor tabledescriptor, String Key, String[] colnames, String[] colvalues){
		if (colnames.length != colvalues.length ){
			System.err.println("Incorrect add schema, colkeys != colvalues");
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
	
	public Result getResult(HTableDescriptor tableDescriptor, String rowKey){
		try{
			Table table = conn.getTable(tableDescriptor.getTableName());
			return table.get(new Get(Bytes.toBytes(rowKey)));
		} catch (IOException e){
			e.printStackTrace();
		}
		return null;
	}
	
	public Result[] getResultList(HTableDescriptor tableDescriptor, List<Get> rowKeyList){
		try{
			Table table = conn.getTable(tableDescriptor.getTableName());
			return table.get(rowKeyList);
		} catch (IOException e){
			e.printStackTrace();
		}
		return null;
	}
	
}

package hbs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.yecht.ruby.Scalar;

import hbase.DBStrings;
import hbase.HbaseManager2;
import hbase.HbaseUtil;

public class HBSNew implements BlockProtocol {

	HbsUtil util = new HbsUtil();
	HbaseManager2 hbm2 = new HbaseManager2();
	
	static final int getLimit = 100;
	static final int readStoreLimit = 4000;
	
	MaxSizeHashMap<Long, BlockData> readStore = new MaxSizeHashMap<Long, BlockData>(readStoreLimit);
	
	//TODO: reset tailImageKey and headImageKey to null when snapshot happens
	private String _tailImageKey = null;
	private String _headImageKey = null;
	private String _currentImageKey = null;
	
	private String getTailImageKey(String imageKey){
		if(_currentImageKey == null || _currentImageKey != imageKey){
			_tailImageKey = null;
			_headImageKey = null;
			_currentImageKey = imageKey;
		}
		if (_tailImageKey == null){
			Result internaldata = hbm2.getResult(hbm2.internalsTable, imageKey);
			_tailImageKey = Bytes.toString( internaldata.getValue( Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), 
									Bytes.toBytes(DBStrings.Col_internals_tail)) );
		}
		return _tailImageKey;
	}
	
	private String getHeadImageKey(String imageKey){
		if(_currentImageKey == null || _currentImageKey != imageKey){
			_tailImageKey = null;
			_headImageKey = null;
			_currentImageKey = imageKey;
		}
		if (_headImageKey == null){
			Result internaldata = hbm2.getResult(hbm2.internalsTable, imageKey);
			_headImageKey = Bytes.toString( internaldata.getValue( Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), 
									Bytes.toBytes(DBStrings.Col_internals_head)) );
		}
		
		return _headImageKey;
	}
	
	
//	public HBSNew(){
//		setTableConnections();
//	}
//	
//	private void setTableConnections(){
//		if (internals_htable == null){
//			try {
//				internals_htable = new HTable(HbaseUtil.con, DBStrings.Table_hbs_internals_table);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//		if (data_htable == null){
//			try {
//				data_htable = new HTable(HbaseUtil.con, DBStrings.Table_hbs_data_table);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//	}
	
	@Override
	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
		return 1L;
	}

	@Override
	public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String createImage(long size) {
		String newImageKey = util.generateString();
		while(true){
			Result result = hbm2.getResult(hbm2.internalsTable, newImageKey);
			if(result.isEmpty()){
				break;	
			}
			newImageKey = util.generateString();
		}
		String[] colnames = new String[]{DBStrings.Col_internals_size,DBStrings.Col_internals_head,DBStrings.Col_internals_tail};
		String[] colvalues = new String[]{ String.valueOf(size) , "", ""};
		hbm2.AddRowinTable(hbm2.internalsTable, newImageKey , colnames, colvalues );
		return newImageKey;
	}

	@Override
	public File getImage(String imageKey) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean writeBlock(String imageKey, long addr, BlockData block) {	
		String rowKey = util.genRowKey(imageKey, addr);
		String tailImageKey = getTailImageKey(imageKey);
		if (readStore.containsKey(addr)){
			readStore.remove(addr);
		}
		if(!tailImageKey.equals("")){
			//has tail
			String tailRowKey = util.genRowKey(tailImageKey, addr);
			Result r  = hbm2.getResult(hbm2.dataTable, tailRowKey);
			if(r.isEmpty()){
				//tail has no block
				List<BlockData> blockData = readBlock(imageKey,addr);
				byte[] data = blockData.get(0).value;
				// so copy block to tail image
				hbm2.AddRowinTable(hbm2.dataTable, tailRowKey, DBStrings.Col_data, data);
			}
		}
		hbm2.AddRowinTable(hbm2.dataTable, rowKey, DBStrings.Col_data, block.value);
		
		return true;
	}

	@Override
	public List<BlockData> readBlock(String imageKey, long addr) {
		ArrayList<BlockData> a = new ArrayList<BlockData>();
		BlockData fromMem = readStore.get(addr);
		if (fromMem !=null){
//			System.out.println("returning " + addr + " from read store");
			a.add(fromMem);
			return a;
		}
		// else
		String rowKey = util.genRowKey(imageKey, addr);
		ArrayList<Get> rowKeyList = util.genRowKeyList(imageKey, addr);
		Result[] resultList = hbm2.getResultList(hbm2.dataTable, rowKeyList);
		for (int i=0; i<resultList.length; i++){
			Result currResult = resultList[i];
			BlockData b = new BlockData();
			b.addr = addr + i*BlockProtocol.clientBlockSize;
			b.value = currResult.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_data));
//			System.out.println("putting " + b.addr + " into read store");
			readStore.put(b.addr, b);
		}
		Result r = resultList[0];
		
		if(!r.isEmpty()){
			BlockData b = new BlockData();
			b.addr = addr;
			b.value = r.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_data));
			a.add(b);
			return a;	
		}
		else{
			System.out.println("row is empty in read");
			String head = getHeadImageKey(imageKey);
			if (head.equals("")){
				//System.out.println("Cannot find any refernce to read");
				BlockData defblock = HbsUtil.genBlockData(addr);
				a.add(defblock);
				return a;
			}
			else{
				return readBlock(head, addr);
			}
		}
	}

	@Override
	public String takeSnapShot(String imageKey) {
		Result result = hbm2.getResult(hbm2.internalsTable, imageKey);
		long size = Bytes.toLong(result.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_internals_size)));
		String newImageId = createImage(size);
		String tailImageId = Bytes.toString(result.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_internals_tail)));
		if ( !tailImageId.equals("") ){
			try{
				hbm2.AddRowinTable(hbm2.internalsTable, tailImageId, DBStrings.Col_internals_head, newImageId );
				hbm2.AddRowinTable(hbm2.internalsTable, newImageId, DBStrings.Col_internals_tail, tailImageId );
			}
			catch (Exception e){
				System.out.println("error in part 1 taking snap shot");
				e.printStackTrace();
			}
		}
		// if doesn't contain tail, create a tail
		try{
			hbm2.AddRowinTable(hbm2.internalsTable, imageKey, DBStrings.Col_internals_tail, newImageId );
			hbm2.AddRowinTable(hbm2.internalsTable, newImageId, DBStrings.Col_internals_head, imageKey );
		}
		catch (Exception e){
			System.out.println("error in part 2 taking snap shot");
			e.printStackTrace();
		}
		return newImageId;
	}

	@Override
	public boolean extendImage(String imageKey, long newSize) {
		Result result  = hbm2.getResult(hbm2.internalsTable, imageKey);
		if( result.isEmpty()){
			//no image to extend
			return false;
		}
		long size = Bytes.toLong(result.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), 
												Bytes.toBytes(DBStrings.Col_internals_size)) );
		if (newSize < size){
			// newSize < size cannot extend
			return false;
		}
		else if( newSize == size){
			return true;
		}
		hbm2.AddRowinTable(hbm2.internalsTable, imageKey, DBStrings.Col_internals_size, String.valueOf(newSize) );
		return true;
	}

	@Override
	public boolean trimImage(String imageKey, long newSize) {
		Result result = hbm2.getResult(hbm2.internalsTable, imageKey);
		if( result.isEmpty()){
			//no image to extend
			return false;
		}
		long size = Bytes.toLong(result.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), 
												Bytes.toBytes(DBStrings.Col_internals_size)) );
		if (newSize > size){
			// newSize > size cannot extend
			return false;
		}
		else if( newSize == size){
			return true;
		}
		hbm2.AddRowinTable(hbm2.internalsTable, imageKey, DBStrings.Col_internals_size, String.valueOf(newSize) );
		HbaseUtil.deleteWithPrefix(DBStrings.Table_hbs_data_table, imageKey, newSize);
//		DeleteRowswithPrefix(hbm2.dataTable, imageKey, newSize);
		return true;
	}

	@Override
	public boolean deleteImage(String imageKey) {
		hbm2.DeleteRowinTable(hbm2.internalsTable, imageKey);
		HbaseUtil.deleteImageEntries(DBStrings.Table_hbs_data_table, imageKey);
//		DeleteRowswithPrefix(hbm2.dataTable, imageKey, 0);
		return false;
	}

	@Override
	public boolean commit(String imageKey) {
		return true;
	}
	

}

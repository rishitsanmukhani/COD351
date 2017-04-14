package hbs;

import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.File;
import java.util.List;


public interface BlockProtocolNew extends VersionedProtocol{
		
	public static final long versionID = 1L;
	
	public String createImage(long size, long clientBlockSize, long hadoopBlockSize);
	
	public File getImage(String imageKey ); 
	
	public boolean writeBlock(String imageKey, long addr, BlockData block );
	
	public List<BlockData> readBlock(String imageKey, long addr);
	
	public String takeSnapShot(String imageKey);
	
	public boolean extendImage(String imageKey, long  newSize);
	
	public boolean trimImage(String imageKey, long  newSize);

	public boolean deleteImage(String imageKey);
	
	public boolean commit(String imageKey);
	
}

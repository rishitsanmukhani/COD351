package hbs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.ProtocolSignature;

public class HBSv5 implements BlockProtocolNew{
	
	public HblockConnectionManager hblockConnectionManager;
	
	public ImageMemMeta imageMemMeta;
	private String user = "testuser";
	
	public HBSv5(ImageMemMeta imageMemMeta, HblockConnectionManager hblockConnectionManager) {
		this.hblockConnectionManager = hblockConnectionManager;
		this.imageMemMeta = imageMemMeta;
	}
	
	// follow with createImage
	public HBSv5(HblockConnectionManager hblockConnectionManager) {
		this.hblockConnectionManager = hblockConnectionManager;
	}
	
	@Override
	public ProtocolSignature getProtocolSignature(String arg0, long arg1, int arg2) throws IOException {
		return null;
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return 5L;
	}

	@Override
	public String createImage(long size, long clientBlockSize, long hadoopBlockSize) {
		imageMemMeta = hblockConnectionManager.createImageMemMeta( user, size, clientBlockSize, hadoopBlockSize);
		// TODO Save the image Data in a file
//		System.out.println("")
		return imageMemMeta.imageKey;
	}

	@Override
	public File getImage(String imageKey) {
		// TODO Auto-generated method stub
		return null;
	}

	
	@Override
	public boolean writeBlock(String imageKey, long addr, BlockData block) {
		imageMemMeta.readCache.remove(addr);
		if(addr > imageMemMeta.imageSize){
			System.out.println("addr greater than image size");
			return false;
		}
		FSDataOutputStream appendStream = imageMemMeta.tempAppendStream;
		try {
			long currPosition = imageMemMeta.currentAppendStreamPos + imageMemMeta.writeCacheNumElements*(12+imageMemMeta.clientBlockSize);
			if (appendStream == null){
				System.out.println("opening temp append stream");
				imageMemMeta.openTempAppendStream();
				appendStream = imageMemMeta.tempAppendStream;
			}
			if(currPosition + 12 + imageMemMeta.clientBlockSize > imageMemMeta.maxFileSize){
				imageMemMeta.commitTempUpdates();
				imageMemMeta.SetNextAppendStream();
				imageMemMeta.openTempAppendStream();
				appendStream = imageMemMeta.tempAppendStream;
			}
			else if (imageMemMeta.writeCacheNumElements >= imageMemMeta.WRITE_CACHE_LIMIT){
//				System.out.println("committing temp");
				imageMemMeta.commitTempUpdates();
				imageMemMeta.openTempAppendStream();
				appendStream = imageMemMeta.tempAppendStream;

			}
			AddrDataMapValue location =  imageMemMeta.blocksDirectory.get(addr);
			int newSeqNum = 0;
			if (location != null){
				newSeqNum = location.seqNumber + 1;
			}
			WriteCacheValue oldval = imageMemMeta.writeCache.put(addr, new WriteCacheValue(newSeqNum, block));
			if(oldval == null ){
				imageMemMeta.writeCacheNumElements++;
			}
			appendStream.writeLong(addr);
			appendStream.writeInt(newSeqNum);
			appendStream.write(block.value);
			appendStream.hflush();
//			System.out.println("addr: " + addr + " position : " + (currPosition + 8) + " file: " + imageMemMeta.currentFile);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public void prefetchBlocksToCache(String imageKey, long addr) {
		if (imageMemMeta.readCache.containsKey(addr)){
			return;
		}
		if(addr > imageMemMeta.imageSize){
			System.out.println("addr greater than image size");
			return ;
		}
		AddrDataMapValue location =  imageMemMeta.blocksDirectory.get(addr);
		BlockData def = HbsUtil2.genDefaultBlockData(addr, imageMemMeta.clientBlockSize);
		if(location == null ){
//			System.out.println("location is null so skipping prefetch ");
			return;
		}
		FSDataInputStream inpStream = imageMemMeta.readStreams.get(location.fileNumber);
		try {
			int written = inpStream.read(location.addrOffset,def.value , 0, (int)imageMemMeta.clientBlockSize );
		} catch (IOException e) {
			e.printStackTrace();
		}
//		System.out.println(" inserting " + addr + " into user Cache");
		imageMemMeta.readCache.put(addr, def);
	}

	
	@Override
	public List<BlockData> readBlock(String imageKey, long addr) {
		if(addr > imageMemMeta.imageSize){
			System.out.println("addr greater than image size");
			return null;
		}
		ArrayList<BlockData> ans = new ArrayList<BlockData>();
		BlockData toRetFromCache = imageMemMeta.readCache.get(addr);
		if (toRetFromCache != null){
			imageMemMeta.readCacheHit++;
//			System.out.println("returing from read cache "+ addr);
			ans.add(toRetFromCache);
			return ans;
		}
		else{
			WriteCacheValue toRetFromWriteCache = imageMemMeta.writeCache.get(addr);
			
			if (toRetFromWriteCache != null){
				
				imageMemMeta.writeCacheHit++;
//				System.out.println("returing from write cache "+ addr);
				imageMemMeta.readCache.put(addr, toRetFromWriteCache.blockData);
				/*TODO can do better, instead of storing it now in readCache, 
				 * transfer this element to readCache when it gets removed from writeCache, i.e, on tempCommit
				 * this way no double usage of mem, mem being the most constrained resource.
				 * Also can fetch neighbouring elements similar to goto file case.
				 * */ 
				ans.add(toRetFromWriteCache.blockData);
				return ans;
			}
		}
		AddrDataMapValue location =  imageMemMeta.blocksDirectory.get(addr);
		BlockData def = HbsUtil2.genDefaultBlockData(addr, imageMemMeta.clientBlockSize);
		if(location == null ){
//			System.out.println("location is null for addr: " + addr);
			ans.add(def);
			return ans;
		}
		FSDataInputStream inpStream = imageMemMeta.readStreams.get(location.fileNumber);
		try {
//			if (imageMemMeta.currentFile == location.fileNumber){
//				imageMemMeta.closeAppendStream();
//				inpStream.close();
//				inpStream = hblockConnectionManager.fs.open(new Path(hblockConnectionManager.prefix + imageMemMeta.imageUser + "/" + imageKey + "/" + location.fileNumber));
//				imageMemMeta.readStreams.add(location.fileNumber, inpStream);				
//			}
//			System.out.println("inpstream" + inpStream.available() + " " + location.addrOffset + " " + location.fileNumber);
			int written = inpStream.read(location.addrOffset,def.value , 0, (int)imageMemMeta.clientBlockSize );
			imageMemMeta.readCache.put(addr, def);
			for (int i=1; i<8; i++){
				prefetchBlocksToCache(imageKey, addr+i*imageMemMeta.clientBlockSize);
			}
//			System.out.println(" written : " + location.addrOffset + " " + location.fileNumber + " " + written);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ans.add(def);
		return ans;
	}

	@Override
	public String takeSnapShot(String imageKey) {
		try {
			Path currPath = new Path(hblockConnectionManager.prefix + imageMemMeta.imageUser + "/" + imageMemMeta.imageKey + "/");
			hblockConnectionManager.hdfsAdmin.allowSnapshot(currPath);
			String newPath = hblockConnectionManager.fs.createSnapshot(currPath).toString();
			return newPath;
			//TODO refine. This is not the imagKey, this is just the location of the snapshot in hdfs. Snapshots are no longer images. They are readonly.
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return null;
	}

	@Override
	public boolean extendImage(String imageKey, long newSize) {
		if(newSize >= imageMemMeta.imageSize){
			imageMemMeta.imageSize = newSize;
			return true;
		}
		return false;
	}

	@Override
	public boolean trimImage(String imageKey, long newSize) {
		//TODO in background job check if the image is actually trimmed.
		if(newSize <= imageMemMeta.imageSize){
			imageMemMeta.imageSize = newSize;
			return true;
		}
		return false;
	}

	@Override
	public boolean deleteImage(String imageKey) {
		try {
			imageMemMeta.currentAppendStream.close();
			for(int i=0; i< imageMemMeta.readStreams.size(); i++){
				imageMemMeta.readStreams.get(i).close();
			}
			Path oldPath = new Path(hblockConnectionManager.prefix + imageMemMeta.imageUser + "/" + imageMemMeta.imageKey + "/");
			hblockConnectionManager.fs.delete(oldPath, false);
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public boolean commit(String imageKey) {
//		imageMemMeta.reOpenAppendStream();
		return true;	
	}

}

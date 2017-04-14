package hbs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

public class Demonstration {

	private static long loadFile(HBSv5 client, String imageKey, long startAddr, String path){
		Path filePath = Paths.get(path);
		try {
			byte[] data = Files.readAllBytes(filePath);
			int nBlocks = (int) (data.length/HBSv5Test.clientBlockSize) + 1;
			long fileLen = data.length;
			for (int i=0; i<nBlocks; i++){
				long addr = i*HBSv5Test.clientBlockSize;
				BlockData b = HbsUtil2.genDefaultBlockData(addr, HBSv5Test.clientBlockSize);
				if (i==nBlocks-1){
					// copy only the last part
					long copySize = fileLen%HBSv5Test.clientBlockSize;
					System.arraycopy(data, (int)addr, b.value, 0, (int)copySize);
				}
				else{
					System.arraycopy(data, (int)addr, b.value, 0, (int)HBSv5Test.clientBlockSize);
				}
//				System.out.println("addr: " + addr + " " +  Arrays.toString(b.value));
				client.writeBlock(imageKey, addr, b);
			}
			return fileLen;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}

	private static void getFileAndSave(HBSv5 client, String imageKey, long startAddr, long fileSize, String filePath){
		byte[] dataNew = new byte[(int)fileSize];
		int nBlocks = (int) (fileSize/HBSv5Test.clientBlockSize) + 1;
		for (int i=0; i<nBlocks; i++){
			if (i!=(nBlocks-1)){
				BlockData b = client.readBlock(imageKey, startAddr + i*HBSv5Test.clientBlockSize).get(0);
				System.arraycopy(b.value, 0, dataNew, (int)(i*HBSv5Test.clientBlockSize), (int) HBSv5Test.clientBlockSize);
			}
			else{
				BlockData b = client.readBlock(imageKey, startAddr + i*HBSv5Test.clientBlockSize).get(0);
				System.arraycopy(b.value, 0, dataNew, (int)(i*HBSv5Test.clientBlockSize), (int) (fileSize%HBSv5Test.clientBlockSize));
			}
		}
		try {
			FileUtils.writeByteArrayToFile(new File(filePath), dataNew);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void retrieveLogo(HblockConnectionManager hblockConnectionManager, String imageKey, long fileSize){
		long startAddr = 0;
		String fileDstPath = "/home/baadalvm/retrievedFile";
		HBSv5 client = hblockConnectionManager.getBlockStore(imageKey);
		getFileAndSave(client, imageKey, startAddr, fileSize, fileDstPath);
	}
	
	private static void writeLogo(HblockConnectionManager hblockConnectionManager, String fileSrcPath){
		HBSv5 client = hblockConnectionManager.createNewBlockStore();
		String imageKey = client.createImage(1<<30, HBSv5Test.clientBlockSize, HBSv5Test.hadoopBlockSize );
		long startAddr = 0;
		long fileSize = loadFile(client, imageKey, startAddr,fileSrcPath) ;
		System.out.println("imageKey : " + imageKey + " " + fileSize);
	}
	
	public static void main(String args[]){
//		HblockConnectionManager hblockConnectionManager = new HblockConnectionManager();
//		writeLogo(hblockConnectionManager, args[0]);
//		retrieveLogo(hblockConnectionManager, args[0], Long.parseLong(args[1]));
	}
	
}

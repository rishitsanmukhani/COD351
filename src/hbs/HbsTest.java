package hbs;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

public class HbsTest {

	static long startTime, endTime;
	static HBSNew h2 = new HBSNew();
	
	private static void read(){
//		List<BlockData> l = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
//		List<BlockData> l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
//		l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
//		l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
//		l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
//		l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
//		l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
//		l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
//		l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
//		l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
//		
//		BlockData b = l.get(0), b1 = l1.get(0);
//		System.out.println( b.addr + " - " + b1.addr);
//
//		for (int i=0; i<b.value.length; i++){
//			System.out.print(b.value[i]);
//		}
//	
//		System.out.println(" ");
//		
//		for (int i=0; i<b1.value.length; i++){
//			System.out.print(b1.value[i]);
//		}

	}

	/*
	 * @param size in mb
	 * */
	private static String createImage(long size){
		long imageSize =100*(1<<20); 
		String imageKey = h2.createImage(imageSize);
		System.out.println(imageKey);
		return imageKey;
	}
	
	
	public static void main(String args[]){
		BenchMarkingNew bm2 = new BenchMarkingNew();
//		String imageKey = bm2.runSeqTest(300);
		bm2.runSeqReadTest(300, "YMQas");
		
		
//		runSeqTest(50);
//		read();
//		String imageKey = createImage(100);
//		System.out.println("image key : " + imageKey);
//		newTest(imageKey);
//		h2.trimImage("HVVAniyEkOcUIL1LCSufZSWYsBkivx28vLVz2qLY92gIZ9wJo6vLIgYN5", 5*BlockProtocol.clientBlockSize);
	//		deleteTest(imageKey);

		/*keys.
		 * 
		 * slave3 - iJ7rTOcGZ5ysfCCYQDlDeTAlJSQA5hEQa3CkayQS1L0t2WB5DTum16g7QUcwggh
		 * slave2 - GCY3sSeBAc4UK65KmIzImgXSzsRZ4ZgU6PpkttFNRnO6uqMA
		 * slave2 - L8
		 * slave2 - BVG
		 * */
	}

	public static void startTimer(){
		System.out.println("starting timer");
		startTime = System.nanoTime();
	}
	
	public static void endTimer(){
		endTime = System.nanoTime();
		System.out.println("stopped Timer");
		long duration = (endTime - startTime);
		System.out.println(" It took " + duration/1000000 + " milliseconds");
	}
	
	/**
	 * size is in MB
	 * @param size
	 */
	public static void runSeqTest(long size){
		
		long imageSize = 1000*(1<<20);
		String imageKey = h2.createImage(imageSize); 
		
		BlockData blockData = HbsUtil.genBlockData(0);
		System.out.println("image : " + imageKey);
				
//		startTimer();
		
		for (int i=0; i<(size<<20)/BlockProtocol.clientBlockSize; i++){
			blockData.addr = i*BlockProtocol.clientBlockSize;
			if (i%1000==0){
				System.out.println(i);
			}
			
			h2.writeBlock(imageKey, blockData.addr, blockData);
		}
//		endTimer();
		
	}
	
	private static void deleteTest(String imageKey){
		h2.deleteImage(imageKey);
	}
	
	private static void newTest(String imageKey){
		long addr = 4*BlockProtocol.clientBlockSize;
		BlockData b = HbsUtil.genBlockData(addr);
		h2.writeBlock(imageKey, addr, b);
		b = HbsUtil.genBlockData(addr);
		for (int i=4; i<100; i++){
			addr = i*BlockProtocol.clientBlockSize;	
			h2.writeBlock(imageKey, addr, b);
			b.value[9] = 1;
			h2.writeBlock(imageKey, addr, b);
		}

		ArrayList<BlockData> ret = (ArrayList<BlockData>)h2.readBlock(imageKey, addr);
		if (ret.size()!=0){
			BlockData b2 = ret.get(0);
			System.out.println("addr : " + b2.addr);
			for (int i=0; i<b2.value.length; i++){
				System.out.print(b2.value[i]);
			}
			System.out.println();
		}
		else{
			System.out.println("returned array of size 0");
		}

	}
	
	public void write(){
//		String imageKey = hbs.createImage(1000); 
//		
//		BlockData blockData = util.genBlockData(0);
//		
//		System.out.println("image : " + imageKey);
//		
//		blockData.addr = 12*BlockProtocol.clientBlockSize;
//		hbs.writeBlock(imageKey, blockData.addr, blockData);
//		hbs.commit(imageKey);
//		hbs.close();
	}
	
}

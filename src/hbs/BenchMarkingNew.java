package hbs;

import java.util.Arrays;

public class BenchMarkingNew {

	long startTime;
	long endTime;
	HBSNew hbs = new HBSNew();
//	HbsUtil util = new HbsUtil();
	String imageName;
	
	public void startTimer(){
		System.out.println("starting timer");
		startTime = System.nanoTime();
	}
	
	public void endTimer(){
		endTime = System.nanoTime();
		System.out.println("stopped Timer");
		long duration = (endTime - startTime);
		System.out.println(" It took " + duration/1000000 + " milliseconds");
	}
	
	/**
	 * size is in MB
	 * @param size
	 */
	public String runSeqTest(long size){
		
		long imageSize = size*(1<<20);
		String imageKey = hbs.createImage(imageSize); 
		imageName = imageKey;
		
		BlockData blockData = HbsUtil.genBlockData(0);
		System.out.println("image : " + imageKey);
				
		startTimer();
		
		for (int i=0; i<(size<<20)/BlockProtocol.clientBlockSize; i++){
			blockData.addr = i*BlockProtocol.clientBlockSize;
			if (i%1000==0){
				System.out.println(i);
			}
			
			hbs.writeBlock(imageKey, blockData.addr, blockData);
		}
		endTimer();
		return imageKey;
	}
	
	public void runSeqReadTest(long size, String imageKey){
		
		BlockData bd = HbsUtil.genBlockData(-1);
		startTimer();
		int limit = (int) ((size<<20)/BlockProtocol.clientBlockSize);
		for (int i=0; i<limit; i++){
			if (i%1000==0) System.out.println(i);
			bd = hbs.readBlock(imageKey, i*BlockProtocol.clientBlockSize).get(0);
		}
		endTimer();
	}

	
	public void averageSeqWrite(int nTries, long size){
		long totTime = 0;
		for (int i=0; i<nTries; i++){
			runSeqTest(size);
			long dur  = endTime - startTime;
			long millis = dur/1000000;
			totTime += millis;
			System.out.println("test " + i + " took " + millis + "ms");
		}
		System.out.println("average Time = " + totTime/nTries);
	}
	
	public void averageSeqRead(int nTries, long size, String imageKey){
		long totTime = 0;
		for (int i=0; i<nTries; i++){
			runSeqReadTest(size, imageKey);
			long dur  = endTime - startTime;
			long millis = dur/1000000;
			totTime += millis;
			System.out.println("test " + i + " took " + millis + "ms");
		}
		System.out.println("average Time = " + totTime/nTries);
	}
	
}

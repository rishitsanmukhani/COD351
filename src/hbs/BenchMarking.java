package hbs;

import java.util.Arrays;

public class BenchMarking {

	long startTime;
	long endTime;
	Hbs hbs = new Hbs();
	HbsUtil util = new HbsUtil();
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
	public void runSeqTest(long size){
		
		String imageKey = hbs.createImage(1000); 
		imageName = imageKey;
		
		BlockData blockData = util.genBlockData(0);
		
		long commitCycle = (30<<20)/BlockProtocol.clientBlockSize;
		
		startTimer();
		System.out.println("image : " + imageKey);
		System.out.println("limit : " + (size<<20)/BlockProtocol.clientBlockSize);
		
		for (int i=0; i<(size<<20)/BlockProtocol.clientBlockSize; i++){
//		for (int i=0; i<40; i++){
			blockData.addr = i*BlockProtocol.clientBlockSize;
//			System.out.println("addr: " + blockData.addr);
			if (i%1000==0){
				System.out.println(i);
			}
			
			if (i%commitCycle == (commitCycle-1)){
				hbs.commit(imageKey);
			}
			
			hbs.writeBlock(imageKey, blockData.addr, blockData);
		}
		hbs.close();
//		hbs.commit(imageKey);
		endTimer();
		
	}
	
	public void runSeqReadTest(long size, String imageKey){
		
		BlockData bd = util.genBlockData(-1);
		hbs.initForImage(imageKey);
		startTimer();
		int limit = (int) ((size<<20)/BlockProtocol.clientBlockSize);
		for (int i=0; i<limit; i++){
			bd = hbs.readBlock(imageKey, i*BlockProtocol.clientBlockSize).get(0);
//			System.out.println(bd.addr + " = "+ Arrays.toString(bd.value));
		}
		hbs.close();
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

package hbs;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import io.netty.util.internal.ThreadLocalRandom;

public class HBSv5Test {

	public static long clientBlockSize = HblockConnectionManager.DEFAULT_CLIENT_BLOCK_SIZE;
	public static long hadoopBlockSize = HblockConnectionManager.DEFAULT_HADOOP_BLOCK_SIZE;
	
	private static void write(HBSv5 client, String imageKey){
		for (int i=0; i<16000; i++){
			if  (i%1000==0) System.out.println(i);
			client.writeBlock(imageKey, i*clientBlockSize, HbsUtil2.genDefaultBlockData2(i*clientBlockSize, clientBlockSize));
		}
	}

	private static void write(HBSv5 client, String imageKey, int nBlocks){
		for (int i=0; i<nBlocks; i++){
			if  (i%1000==0) System.out.println(i);
			client.writeBlock(imageKey, i*clientBlockSize, HbsUtil2.genDefaultBlockData2(i*clientBlockSize, clientBlockSize));
		}
	}
	
	private static void seqRead(HBSv5 client, String imageKey){
		for (int i=0; i<16000; i++){
			BlockData b = client.readBlock(imageKey, i*clientBlockSize).get(0);
			if (i%400==0){
//				HbsUtil2.printBlockData(b);
			}
		}
	}
	
	private static void writeReadTest(HBSv5 client, String imageKey){
		for (int i=0; i<1000; i++){
			client.writeBlock(imageKey, i*clientBlockSize, HbsUtil2.genDefaultBlockData2(i*clientBlockSize, clientBlockSize));
		}
		System.out.println(" initial write done");
		System.out.println(System.currentTimeMillis());
		for (int i=1000; i<3000; i++){
			client.writeBlock(imageKey, i*clientBlockSize, HbsUtil2.genDefaultBlockData2(i*clientBlockSize, clientBlockSize));
			BlockData b = client.readBlock(imageKey, (i-1000)*clientBlockSize).get(0);
		}
	}	
	
	private static void seqReadTest(){
		HblockConnectionManager hblockConnectionManager = new HblockConnectionManager();
		
		final HBSv5 client1 = hblockConnectionManager.createNewBlockStore();
		final String imageKey1 = client1.createImage(1<<30, clientBlockSize, hadoopBlockSize );
		System.out.println("imageKey " + imageKey1);
			

		final HBSv5 client2 = hblockConnectionManager.createNewBlockStore();
		final String imageKey2 = client2.createImage(1<<30, clientBlockSize, hadoopBlockSize );
		System.out.println("imageKey " + imageKey2);
		
		final HBSv5 client3 = hblockConnectionManager.createNewBlockStore();
		final String imageKey3 = client3.createImage(1<<30, clientBlockSize, hadoopBlockSize );
		System.out.println("imageKey " + imageKey3);
		
		write(client1, imageKey1);
		write(client2, imageKey2);
		write(client3, imageKey3);
			
		new Thread(new Runnable(){
			public void run(){
				long a = System.currentTimeMillis();
				seqRead(client1, imageKey1);
				seqRead(client1, imageKey1);
				long b = System.currentTimeMillis();				
				System.out.println( "Thread 1: " + (float)(b-a)/1000.0 + " seconds");
			}
		}).start();

		new Thread(new Runnable(){
			public void run(){
				long a = System.currentTimeMillis();
				seqRead(client2, imageKey2);
				seqRead(client2, imageKey2);
				long b = System.currentTimeMillis();				
				System.out.println( "Thread 2: " + (float)(b-a)/1000.0 + " seconds");
			}
		}).start();
		
		new Thread(new Runnable(){
			public void run(){
				long a = System.currentTimeMillis();
				seqRead(client3, imageKey3);
				seqRead(client3, imageKey3);
				long b = System.currentTimeMillis();				
				System.out.println( "Thread 3: " + (float)(b-a)/1000.0 + " seconds");
			}
		}).start();
		
	}
	
	private static void seqReadTest(int nThreads){
		HblockConnectionManager hblockConnectionManager = new HblockConnectionManager();
		
		for (int i=0; i<nThreads; i++){
			final HBSv5 client = hblockConnectionManager.createNewBlockStore();
			final String imageKey = client.createImage(1<<30, clientBlockSize, hadoopBlockSize );
			System.out.println("imageKey " + imageKey);
			
		
			final int currThreadNo = i+1;
			
			new Thread(new Runnable(){
				public void run(){
					write(client, imageKey);
					long a = System.currentTimeMillis();
					seqRead(client, imageKey);
					seqRead(client, imageKey);
					seqRead(client, imageKey);
					long b = System.currentTimeMillis();				
					System.out.println( "Thread " + currThreadNo + ": " + (float)(b-a)/1000.0 + " seconds");
					
				}
			}).start();
		}
	}
	
	private static void randomRead(HBSv5 client, String imageKey, int [] order, int nBlocks){
		for (int i=0; i<nBlocks; i++){
			BlockData b = client.readBlock(imageKey, order[i]*clientBlockSize).get(0);
			if (i%400==0){
//				HbsUtil2.printBlockData(b);
			}
		}
	}
	
	private static void write(HBSv5 client, String imageKey, int[] order, int nBlocks){
		for (int i=0; i<nBlocks; i++){
			client.writeBlock(imageKey, order[i]*clientBlockSize, HbsUtil2.genDefaultBlockData2(order[i]*clientBlockSize, clientBlockSize));
		}
	}
	
	private static void randomWriteTest(int nThreads){
		final HblockConnectionManager hblockConnectionManager = new HblockConnectionManager();
		
		for (int i=0; i<nThreads; i++){
			final HBSv5 client = hblockConnectionManager.createNewBlockStore();
			final String imageKey = client.createImage(1<<30, clientBlockSize, hadoopBlockSize );
			System.out.println("imageKey " + imageKey);
			
			final int currThreadNo = i+1;
			final int nBlocks = 16000;
			new Thread(new Runnable(){
				public void run(){
					long a = System.currentTimeMillis();
					int[] order = new int[nBlocks];
					for (int i=0; i<order.length; i++){
						order[i] = ThreadLocalRandom.current().nextInt(nBlocks);
					}
					write(client, imageKey, order, nBlocks);
					long b = System.currentTimeMillis();				
					System.out.println( "Thread " + currThreadNo + ": " + (float)(b-a)/1000.0 + " seconds");
					
				}
			}).start();
		}
	}
	
	private static void randomReadTest(){
		final HblockConnectionManager hblockConnectionManager = new HblockConnectionManager();
//		System.out.println("imageKey " + imageKey);
		final int nBlocks = 16000;

		final HBSv5 client1 = hblockConnectionManager.createNewBlockStore();
		final String imageKey1 = client1.createImage(1<<30, clientBlockSize, hadoopBlockSize );
		write(client1, imageKey1, nBlocks); // writing 512 MB

		final HBSv5 client2 = hblockConnectionManager.createNewBlockStore();
		final String imageKey2 = client2.createImage(1<<30, clientBlockSize, hadoopBlockSize );
		write(client2, imageKey2, nBlocks); // writing 512 MB

		final HBSv5 client3 = hblockConnectionManager.createNewBlockStore();
		final String imageKey3 = client3.createImage(1<<30, clientBlockSize, hadoopBlockSize );
		write(client3, imageKey3, nBlocks); // writing 512 MB

		final HBSv5 client4 = hblockConnectionManager.createNewBlockStore();
		final String imageKey4 = client4.createImage(1<<30, clientBlockSize, hadoopBlockSize );
		write(client4, imageKey4, nBlocks); // writing 512 MB
		
		System.out.println("writes done");
		
		new Thread(new Runnable(){
			public void run(){
				int[] order = new int[nBlocks];
				for (int i=0; i<order.length; i++){
					order[i] = ThreadLocalRandom.current().nextInt(nBlocks);
				}
				System.out.println();
				long a = System.currentTimeMillis();
				randomRead(client1, imageKey1, order, nBlocks);
				long b = System.currentTimeMillis();				
				System.out.println( "Thread 1: " + (float)(b-a)/1000.0 + " seconds");
				
			}
		}).start();

		
		new Thread(new Runnable(){
			public void run(){
				int[] order = new int[nBlocks];
				for (int i=0; i<order.length; i++){
					order[i] = ThreadLocalRandom.current().nextInt(0, nBlocks);
				}
				System.out.println();
				long a = System.currentTimeMillis();
				randomRead(client2, imageKey2, order, nBlocks);
				long b = System.currentTimeMillis();				
				System.out.println( "Thread 2: " + (float)(b-a)/1000.0 + " seconds");
				
			}
		}).start();
		
		
		new Thread(new Runnable(){
			public void run(){
				int[] order = new int[nBlocks];
				for (int i=0; i<order.length; i++){
					order[i] = ThreadLocalRandom.current().nextInt(0, nBlocks);
				}
				long a = System.currentTimeMillis();
				randomRead(client3, imageKey3, order, nBlocks);
				long b = System.currentTimeMillis();				
				System.out.println( "Thread 3: " + (float)(b-a)/1000.0 + " seconds");
				
			}
		}).start();
		
		
		new Thread(new Runnable(){
			public void run(){
				int[] order = new int[nBlocks];
				for (int i=0; i<order.length; i++){
					order[i] = ThreadLocalRandom.current().nextInt(0, nBlocks);
				}
				long a = System.currentTimeMillis();
				randomRead(client4, imageKey4, order, nBlocks);
				long b = System.currentTimeMillis();				
				System.out.println( "Thread 4: " + (float)(b-a)/1000.0 + " seconds");
				
			}
		}).start();
		
	}
	
	private static void randomReadWriteTest(int nThreads){
		final HblockConnectionManager hblockConnectionManager = new HblockConnectionManager();
		
		for (int i=0; i<nThreads; i++){
			final int currThreadNo = i+1;
			
			new Thread(new Runnable(){
				public void run(){
					HBSv5 client = hblockConnectionManager.createNewBlockStore();
					String imageKey = client.createImage(1<<30, clientBlockSize, hadoopBlockSize );
					System.out.println("imageKey " + imageKey);
					int writeOper = 0, readOper = 0;
					int nOperations = 8000;
					int order[] = new int[nOperations];
					for (int i=0; i<nOperations; i++){
						order[i] = ThreadLocalRandom.current().nextInt(0, 4000);
					}
					int operationOrder[] = new int[nOperations];
					for (int i=0; i<nOperations; i++){
						operationOrder[i] = ThreadLocalRandom.current().nextInt(0, 2);
					}
					long a = System.currentTimeMillis();
					for (int i=0; i<nOperations; i++){
						int curr = operationOrder[i];
						if (curr==0){
							writeOper++;
							client.writeBlock(imageKey, order[i]*clientBlockSize, HbsUtil2.genDefaultBlockData2(i*clientBlockSize, clientBlockSize));
						}
						else{
							readOper++;
							client.readBlock(imageKey, order[i]*clientBlockSize);
						}
					}
					long b = System.currentTimeMillis();
					System.out.println( "Thread " + currThreadNo + ": " + (float)(b-a)/1000.0 + " seconds");

					System.out.println("readOper: " + readOper + " writeOper: " + writeOper);
				}
			}).start();
		}

	}
	
	
	private static void normalizedRandomTest(int nThreads){
		final HblockConnectionManager hblockConnectionManager = new HblockConnectionManager();
		
		for (int i=0; i<nThreads; i++){
//			final HBSv5 client1 = hblockConnectionManager.createNewBlockStore();
			final int currThreadNo = i+1;
			
			new Thread(new Runnable(){
				public void run(){
					HBSv5 client = hblockConnectionManager.createNewBlockStore();
					String imageKey = client.createImage(1<<30, clientBlockSize, hadoopBlockSize );
					System.out.println("imageKey " + imageKey);
					int writeOper = 0, readOper = 0;
					int nOperations = 32000;
					int order[] = new int[nOperations];
					Random r = new Random();
					int addressMin = 0;
					int addressMax = nOperations/2;
					for (int i=0; i<nOperations; i++){
						order[i] = (int)( r.nextGaussian()*((double)addressMax/6.0) + (addressMin + addressMax)/2);
						if (order[i]>addressMax || order[i]<addressMin){
							order[i] = ThreadLocalRandom.current().nextInt(0, addressMax);
						}
//						System.out.print(order[i] + " ");
					}
//					System.out.println();
					int operationOrder[] = new int[nOperations];
					for (int i=0; i<nOperations; i++){
						operationOrder[i] = ThreadLocalRandom.current().nextInt(0, 2);
					}
					// filling 1GB of data
					write(client, imageKey, nOperations/2);
					long a = System.currentTimeMillis();
					for (int i=0; i<nOperations; i++){
						int curr = operationOrder[i];
						if (curr==0){
							writeOper++;
							client.writeBlock(imageKey, order[i]*clientBlockSize, HbsUtil2.genDefaultBlockData2(i*clientBlockSize, clientBlockSize));
						}
						else{
							readOper++;
							client.readBlock(imageKey, order[i]*clientBlockSize);
						}
					}
					long b = System.currentTimeMillis();
					System.out.println( "Thread " + currThreadNo + ": " + (float)(b-a)/1000.0 + " seconds");
					System.out.println(client.imageMemMeta.readCacheHit + " " + client.imageMemMeta.writeCacheHit);
					System.out.println("readOper: " + readOper + " writeOper: " + writeOper);
				}
			}).start();
		}
	}
	
	private static void fillSmallData(HblockConnectionManager hblockConnectionManager){
		final HBSv5 client1 = hblockConnectionManager.createNewBlockStore();
		final String imageKey1 = client1.createImage(1<<30, clientBlockSize, hadoopBlockSize );
		System.out.println("imageKey1 " + imageKey1);
		
		new Thread(new Runnable(){
		public void run(){
			long a = System.currentTimeMillis();
			write(client1, imageKey1, 1000);
			long b = System.currentTimeMillis();
			System.out.println( "Thread 1: " + (float)(b-a)/1000.0 + " seconds");
			
		}
	}).start();

	}
	
	private static void test(){
		Path dataFilePath = new Path("/user/Hblock/v0.5/testuser/r33Ra5igCUtPkTgty/0_temp");
		System.out.println("			for data file " + dataFilePath.toString());

		long currPos = 0;
		FSDataInputStream dataFileStream = null;
		long unitSize = 12 + clientBlockSize;
		try {
			dataFileStream = HbsUtil2.getFileSystem().open(dataFilePath);
			int counter = 0;
			while(true){
				System.out.println("in while loop");
				dataFileStream.seek(counter*unitSize);
				long addr = dataFileStream.readLong();
				dataFileStream.seek(counter*unitSize + 8);
				int seqNum = dataFileStream.readInt();
				dataFileStream.seek(counter*unitSize + 12);
				byte[] blockBuffer = new byte[(int)clientBlockSize];
				int totalRead = 0;
				while(totalRead<clientBlockSize){
					int currRead = dataFileStream.read(totalRead, blockBuffer, totalRead, (int)clientBlockSize - totalRead);
					totalRead += currRead;
				}
				System.out.println("		adding " + addr + " to 0 " + seqNum + " " + counter);
				currPos += unitSize;
				counter++;
			}									
		} catch (Exception e) {
			System.out.println("exiting loop because of end of file");
			if (dataFileStream != null){
				try{
					dataFileStream.close();								
				} catch (Exception e1){
					e1.printStackTrace();
				}
			}
		}

		
	}
	
	public static void main(String args[]){

//		test();
		
		HblockConnectionManager hblockConnectionManager = new HblockConnectionManager();
	
		
//		seqReadTest();
//		randomReadTest();
//		randomWriteTest(4);
//		randomReadWriteTest(6);
		normalizedRandomTest(3);
		

		
//		final HBSv5 client3 = hblockConnectionManager.createNewBlockStore();
//		final String imageKey3 = client3.createImage(1<<30, clientBlockSize, hadoopBlockSize );
//		System.out.println("imageKey1 " + imageKey3);
//
//		final HBSv5 client4 = hblockConnectionManager.createNewBlockStore();
//		final String imageKey4 = client4.createImage(1<<30, clientBlockSize, hadoopBlockSize );
//		System.out.println("imageKey1 " + imageKey4);
//
//		final HBSv5 client5 = hblockConnectionManager.createNewBlockStore();
//		final String imageKey5 = client5.createImage(1<<30, clientBlockSize, hadoopBlockSize );
//		System.out.println("imageKey1 " + imageKey5);
//
//		final HBSv5 client6 = hblockConnectionManager.createNewBlockStore();
//		final String imageKey6 = client6.createImage(1<<30, clientBlockSize, hadoopBlockSize );
//		System.out.println("imageKey1 " + imageKey6);
//

//		new Thread(new Runnable(){
//			public void run(){
//				long a = System.currentTimeMillis();
//				write(client2, imageKey2);
//				long b = System.currentTimeMillis();
//				System.out.println( "Thread 2: " + (float)(b-a)/1000.0 + " seconds");
//				
//			}
//		}).start();
//		
//		new Thread(new Runnable(){
//			public void run(){
//				long a = System.currentTimeMillis();
//				write(client3, imageKey3);
//				long b = System.currentTimeMillis();
//				System.out.println( "Thread 3: " + (float)(b-a)/1000.0 + " seconds");
//				
//			}
//		}).start();
//		
//		new Thread(new Runnable(){
//			public void run(){
//				long a = System.currentTimeMillis();
//				write(client4, imageKey4);
//				long b = System.currentTimeMillis();
//				System.out.println( "Thread 4: " + (float)(b-a)/1000.0 + " seconds");
//				
//			}
//		}).start();
//		
//		new Thread(new Runnable(){
//			public void run(){
//				long a = System.currentTimeMillis();
//				write(client5, imageKey5);
//				long b = System.currentTimeMillis();
//				System.out.println( "Thread 5: " + (float)(b-a)/1000.0 + " seconds");
//				
//			}
//		}).start();
//		
//		new Thread(new Runnable(){
//			public void run(){
//				long a = System.currentTimeMillis();
//				write(client6, imageKey6);
//				long b = System.currentTimeMillis();
//				System.out.println( "Thread 6: " + (float)(b-a)/1000.0 + " seconds");
//				
//			}
//		}).start();
		
//		write(client1, imageKey1);
//		System.out.println(System.currentTimeMillis());
//		
//		for (int i=0; i<2000; i++){
//			BlockData b = client1.readBlock(imageKey1, i*clientBlockSize).get(0);
//			if (i%100==0) HbsUtil2.printBlockData(b);
//		}
//		System.out.println(System.currentTimeMillis());
//		
//		System.out.println("read 1 done");
		
//		for (int i=0; i<100; i++){
//			BlockData b = client2.readBlock(imageKey1, i*clientBlockSize).get(0);
////			HbsUtil2.printBlockData(b);
//		}
//		System.out.println(System.currentTimeMillis());

//		HBSv5 client2 = hblockConnectionManager.createNewBlockStore();
//		client2.createImage(0, clientBlockSize, hadoopBlockSize );
		
	}
}

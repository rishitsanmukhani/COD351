package tests;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.net.URI; 
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import hbs.Hbs;
import hos.ObjectInfo;
import restCalls.*;

public class Test {
	
			
	public static void main(String args[]) throws IllegalArgumentException, IOException{
		Hbs hbs = new Hbs();
		PutObject p = new PutObject();
		CreateBucket c = new CreateBucket();
		DeleteBucket d = new DeleteBucket();
		DeleteObject d2 = new DeleteObject();
		GetObject g = new GetObject();
		ListBuckets l = new ListBuckets();
		ListObjectsInBucket lo =  new ListObjectsInBucket();
		
//		readFromArchive("/user/hduser/trial/foo.har", "12");
		
//		LogfileManager lfm = new LogfileManager(utilities.getFileSystem());
//		BenchMarking bm = new BenchMarking();
//		
//		long size = 100;
//		
//		for (int i=0; i<4; i++){
//			bm.runSeqTest(size);
//			bm.runSeqReadTest(size, bm.imageName);			
//		}

//		System.out.println(hfs.getFileStatus(new Path("har:///user/hduser/trial/foo.har/a1")));
//		FileSystem fs = utilities.getFileSystem();
//		HarFileSystem hfs = new HarFileSystem(fs);
//		FSDataOutputStream fsout = hfs.create(new Path("/user/hduser/trial2"));
//		fsout.writeInt(123);
//		fsout.close();
		//		System.out.println(utilities.getFileSystem());
		
		
		
//		HashMap<Long, List<String> > h = new HashMap<Long, List<String> >();
//		ArrayList a = new ArrayList();
//		a.add("1");
//		a.add("2");
//		h.put((long)1, a);
//		a.add("3");
//		List b= h.get((long)1);
//		for (int i=0; i<b.size(); i++){
//			System.out.println(b.get(i));
//		}
		
		
		
//		BlockData bd1 = utilities.genBlockData(12*BlockProtocol.hadoopBlockSize);
//		BlockData bd2 = utilities.genBlockData(12*BlockProtocol.hadoopBlockSize + BlockProtocol.clientBlockSize);
//		bd2.value[2] = 1;
//
		//		b.createImage(1000);
//		utilities.getFileSystem().delete(new Path(imageKey + "/logFile"));
//		lfm.appendtoLog(imageKey, bd1);
//		lfm.appendtoLog(imageKey, bd2);
//
//		hbs.commit(imageKey);
//		List<BlockData> ret1 = hbs.readBlock(imageKey, 12*BlockProtocol.hadoopBlockSize);
//		if (ret1.size()>0){
//			BlockData ret11 = ret1.get(0);
//			System.out.println(ret11.addr + " : " + Arrays.toString(ret11.value));
//		}
//		
//		List<BlockData> ret2 = hbs.readBlock(imageKey, 12*BlockProtocol.hadoopBlockSize + BlockProtocol.clientBlockSize);
//		if (ret2.size()>0){
//			BlockData ret21 = ret2.get(0);
//			System.out.println(ret21.addr + " : " + Arrays.toString(ret21.value));
//		}
//		
//		
//		BlockData readData = lfm.readFromLog(imageKey, bd1.addr);
//		
//		System.out.println("test1");
//		for (int i=0; i<bd1.value.length; i++){
//			if (bd1.value[i]!=readData.value[i]){
//				System.out.println("difference at index: " + i);
//			}
//		}
//		
//		System.out.println("test2");
//		for (int i=0; i<bd2.value.length; i++){
//			if (bd2.value[i]!=readData.value[i]){
//				System.out.println("difference at index: " + i);
//			}
//		}
				System.out.println("tests done");
		
		////////////////////////////////
//		snapshot test
//		b.takeSnapShot("bURkmckjHorGjusO112ciTIBmHiukXt9p2vlIil");
//		System.out.println(utilities.getImageSize(b.fs, "7TX2Yolhutcv7AebzD1HClFhh54p3QwgRmon5iO6KuiEvQwjssBPbJMXBaKnl"));
//		BlockData bd = new BlockData();
//		bd.addr = 16*(1<<24);
//		bd.value = new byte[(int) BlockProtocol.clientBlockSize];
//
//		bd.value[1] = 1;
//		bd.value[6] = 1;
//		b.writeBlock("bURkmckjHorGjusO112ciTIBmHiukXt9p2vlIil", 16*(1<<24), bd);

		
		///////////////////////////////
		
		
//		long l = 
//		utilities.byteToLong(b.fs.getXAttr(new Path("roRHC"), "user.size") );
		
//		BlockData bd = new BlockData();
//		bd.addr = 16*(1<<24);
//		bd.value = new byte[(int) BlockProtocol.clientBlockSize];
//
//		bd.value[1] = 1;
//		bd.value[5] = 1;
//		b.writeBlock("bURkmckjHorGjusO112ciTIBmHiukXt9p2vlIil", 16*(1<<24), bd);
		
//		List<BlockData> lbd = b.readBlock("ulUd1DFQ7HAOHRUVwbcMEczwA9r3MJCgZlvejA", 22*(1<<24));
//		for (int i=0; i<lbd.size(); i++){
//			if (lbd.get(i).addr == (22*(1<<24))){
//				System.out.println(lbd.get(i).value[0]);
//			}
//		}
	}
	
}

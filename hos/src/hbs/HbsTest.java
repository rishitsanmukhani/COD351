package hbs;

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

public class HbsTest {

	static Hbs hbs = new Hbs();
	HbsUtil util = new HbsUtil();
	
	public static void main(String args[]){
		List<BlockData> l = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
		List<BlockData> l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
		l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
		l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
		l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
		l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
		l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
		l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
		l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
		l1 = hbs.readBlock("YQHovaSFEQe8Jd", 12*BlockProtocol.clientBlockSize);
		
		BlockData b = l.get(0), b1 = l1.get(0);
		System.out.println( b.addr + " - " + b1.addr);

		for (int i=0; i<b.value.length; i++){
			System.out.print(b.value[i]);
		}
	
		System.out.println(" ");
		
		for (int i=0; i<b1.value.length; i++){
			System.out.print(b1.value[i]);
		}

	}


	
	public void write(){
		String imageKey = hbs.createImage(1000); 
		
		BlockData blockData = util.genBlockData(0);
		
		System.out.println("image : " + imageKey);
		
		blockData.addr = 12*BlockProtocol.clientBlockSize;
		hbs.writeBlock(imageKey, blockData.addr, blockData);
		hbs.commit(imageKey);
		hbs.close();
	}
	
}

package hbs;

public class BlockData {

	long addr;
	byte[] value;
	
	public BlockData(){
		
	}
	
	public BlockData(long addr, byte[] value){
		this.addr = addr;
		this.value = value;
	}

}

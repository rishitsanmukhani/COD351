package hbs;

public class AddrDataMapValue {
	short fileNumber;
	int seqNumber;
	long addrOffset;
	
	public AddrDataMapValue(short fileNumber, int seqNumber, long addrOffset){
		this.fileNumber = fileNumber;
		this.seqNumber = seqNumber;
		this.addrOffset = addrOffset;
	}
	
}

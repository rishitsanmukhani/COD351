package hos;

import java.io.DataInputStream;

public class ObjectOut {
	public ObjectInfo objectInfo;
	public DataInputStream dis;
	public String uMetaData;
	
	public ObjectOut(ObjectInfo o, DataInputStream d, String uMetaData){
		this.dis = d;
		this.objectInfo = o;
		this.uMetaData = uMetaData;
	}
	
}

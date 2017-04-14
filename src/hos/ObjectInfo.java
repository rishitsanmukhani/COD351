package hos;

import java.util.Map;

/**
 *
 * path
 * charEncoding
 * contentType
 * fileName
 *
 */
public class ObjectInfo{
	public String path;
	public String characterEnconding;
	public String contentType;
	public String fileName;
	public int lenofFile;
	public Map<String,String[]> uMetadata = null;
	
	/**
	 * 
	 * @param a
	 * @param b
	 * @param c
	 * @param d
	 */
	public ObjectInfo(String a, String b, String c, String d, int e, Map<String,String[]> f){
		this.path = a;
		this.characterEnconding = b;
		this.contentType = c;
		this.fileName = d;
		this.lenofFile = e;
		this.uMetadata = f;
	}
	
}
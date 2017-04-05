package hos;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletInputStream;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.ProtocolSignature;
import hbs.HbsUtil;
import restCalls.RestUtil;
import hbase.DBStrings;
import hbase.HbaseManager;
import hbase.HbaseUtil;

public class ObjectACL {

	private HashTable userTable;

	public boolean createUser(String username , String password){
	}

	public boolean insertObjectACL(String username, String password, Path path, List<String> aclStringList){
	}

	public boolean updateObjectACL(String username, String password, Path path, List<String> aclStringList){
	}

	public boolean deleteObjectACL(String username, String password, Path path, String aclStringList){
	}

	public AclStatus getObjectACL(String username, Path path){
	}

	public List<AclEntry> listObjectACL(String username, Path path){
	}
}
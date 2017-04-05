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

public class BucketACL {

	// this should be bucket table i.e. bucket mapped to user and password
	private HashTable userTable;

	// create and how to set permission to which bucket?
	public boolean createUser(String username , String password){
		try {
			File userFile = new File("HadoopUser.txt");
			if(!userFile.exists()){
				userFile.createNewFile(); // if file already exists will do nothing 
			}
			FileOutputStream writeFile = new FileOutputStream(userFile, true); 
			if( !userTable.containsKey(username) ){
				userTable.put(username, password);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// how to check the owner of this path
	public boolean insertBucketACL(String username, String password, Path path, List<String> aclStringList){
		try {
			if (userTable.get(username) == password) {
				List<AclEntry> newACLEntryList = parseAclSpec(aclStringList, true);
				modifyAclEntries(path, newACLEntryList);
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	// insert and update are same functions
	public boolean updateBucketACL(String username, String password, Path path, List<String> aclStringList){
		try {
			if (userTable.get(username) == password) {
				List<AclEntry> newACLEntryList = parseAclSpec(aclStringList, true);
				modifyAclEntries(path, newACLEntryList);
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	// how to check the owner of this path
	public boolean deleteBucketACL(String username, String password, Path path, String aclStringList){
		try {
			if (userTable.get(username) == password) {
				List<AclEntry> newACLEntryList = parseAclSpec(aclStringList, false);
				removeAclEntries(path, newACLEntryList);
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	// how to check the authentication of this user & check return
	public AclStatus getBucketACL(String username, Path path){
		try {
			if(aclName existing in the list){
				AclStatus aclStatus = getAclStatus(path);
				return aclStatus;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return NULL;
	}

	// how to check the authentication of this user & check return type
	public List<AclEntry> listBucketACL(String username, Path path){
		try {
			if(aclName existing in the list){
				AclStatus aclStatus = getAclStatus(path);
				List<AclEntry> aclEntries = AclStatus.getEntries();
				return aclEntries;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return NULL;
	}
}
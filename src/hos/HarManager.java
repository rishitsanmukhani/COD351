package hos;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;

import hbs.HbsUtil;

public class HarManager {

	HbsUtil util = new HbsUtil();
/**
 *  Given a parent directory and the list of files to be archived, it creates the har file with the given name
 * @param dirPath
 * 						the parent directory in which the small files are located /user/hduser/dir1/
 * @param smallFiles
 * 						the names of the files that should be archived a,b,c
 * @param harDirPath
 * 						the directory in which the har file should be placed /user/hduser/harOutput/
 * @param harName
 * 						the name of the har file a.har
 */
	public  boolean makeArchive(String dirPath, ArrayList<String> smallFiles, String harDirPath, String harName){
		FileSystem fs = util.getFileSystem();
		try{
			if (!fs.exists(new Path(harDirPath))){
		    	System.out.println("creating path for har directory");
		    	fs.mkdirs(new Path(harDirPath));
		    }
	    } catch (Exception e){
	    	e.printStackTrace();
	    }
	    
	    final URI uri = fs.getUri(); 

	    int nSmallFiles = smallFiles.size();
	    String args[] = new String[nSmallFiles + 5]; // 5 other arguments
	    args[0] = "-archiveName";
	    args[1] = harName;
	    args[2] = "-p";
	    args[3] = dirPath;
	    args[nSmallFiles+4] = harDirPath ;
	    
	    for (int i=0; i<nSmallFiles; i++){
	    	args[i+4] = smallFiles.get(i);
	    }
	    	   
	    JobConf job = new JobConf(HarManager.class);
	    HadoopArchives har = new HadoopArchives(job);
	    try {
			ToolRunner.run(har, args);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * 
	 * @param harPath
	 * 			/user/hduser/trial/foo33.har
	 * @param dirExtract
	 * 			/user/hduser/smallFiles/
	 * @return
	 */
	public boolean unArchiveAll(String harPath, String dirExtract){
		FsShell fsh = new FsShell(util.getConfiguration());
		FileSystem fs = util.getFileSystem();
		try{
			if (!fs.exists(new Path(dirExtract))){
		    	System.out.println("creating path for har extraction");
		    	fs.mkdirs(new Path(dirExtract));
		    }
	    } catch (Exception e){
	    	e.printStackTrace();
	    }
		
		String args[] = new String[3];
		args[0] = "-cp";
		args[1] = "har://" + harPath + "/*" ; // har:///user/hduser/trial/foo33.har/*";
		args[2] = dirExtract;
		try {
			fsh.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}
	
}
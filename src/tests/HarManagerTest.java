package tests;

import java.io.IOException;
import java.util.ArrayList;

import hbs.HbsUtil;
import hos.HarManager;

public class HarManagerTest {
	public static void main(String args[]){
		HarManager hm = new HarManager();
		HbsUtil Util =new HbsUtil();
		ArrayList<String> files = new ArrayList();
		files.add("a1");
		files.add("a2");
		
		files.add("trial.java");
		hm.makeArchive(Util.FSNAME + "/user/hduser/dir1", files , Util.FSNAME + "/user/hduser/trial/", "foo32.har");
		
		hm.unArchiveAll("/user/hduser/trial/foo33.har", "/user/hduser/smallFiles");
		
		System.out.println("done");
	}
}

package restCalls;


import java.io.*;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import hbase.DBStrings;
import hbase.HbaseUtil;
import hbs.HbsUtil;
import hos.HadoopObjectStore;
import hos.ObjectProtocol;
import hos.UserId;

public class ListBuckets extends HttpServlet{

	  HbsUtil util = new HbsUtil();
	
	  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

		  ArrayList<String> bucketList = (ArrayList<String>) HadoopObjectStore.getHadoopObjectStore().ListBuckets(new UserId(DBStrings.USER_NAME));
		  JSONObject resp = new JSONObject();
		  JSONArray bucketValues = new JSONArray();
		  for (int i=0; i<bucketList.size(); i++){
			  JSONObject bucket = new JSONObject();
			  bucket.put(String.valueOf(i+1), bucketList.get(i));
			  bucketValues.add(bucket);
		  }
		  resp.put("bucketList", bucketValues);
		  
		  FileSystem fs = util.getFileSystem();

		  response.setContentType("application/json");
		  response.getWriter().write(resp.toString());;
	
	}
	
}

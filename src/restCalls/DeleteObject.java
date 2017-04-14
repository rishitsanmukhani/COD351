package restCalls;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import hbs.HbsUtil;
import hos.BucketId;
import hos.HadoopObjectStore;
import hos.ObjectId;

public class DeleteObject extends HttpServlet{

	  HbsUtil util = new HbsUtil();
	
	  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
		  	String bucketKey = (String) request.getParameter("bucketKey");
		  	String objectKey = (String) request.getParameter("objectKey");

		  	boolean success =  HadoopObjectStore.getHadoopObjectStore().DeleteObject(new BucketId(bucketKey), new ObjectId(objectKey) );
		  	JSONObject resp = new JSONObject();
		  	String message = "";
		  	if (success){
		  		message = "success";
		  	}
		  	else{
		  		message = "failed";
		  	}
		  	resp.put("response", message);
	  		response.getWriter().write(resp.toString());
	  		
	  }
	  
}

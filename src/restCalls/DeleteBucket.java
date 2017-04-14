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

import hbase.DBStrings;
import hbs.HbsUtil;
import hos.BucketId;
import hos.BucketInfo;
import hos.HadoopObjectStore;
import hos.UserId;

public class DeleteBucket extends HttpServlet{

	  HbsUtil util = new HbsUtil();
	  String message ;
	  
	  public void doDelete(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
		  	String bucketKey = (String) request.getParameter("bucketKey");
		  	message = "";
		  	JSONObject resp = new JSONObject();
		  	HadoopObjectStore h = HadoopObjectStore.getHadoopObjectStore();
		  	BucketId b = new BucketId(bucketKey);
		  	message = h.DeleteBucket(new UserId(DBStrings.USER_NAME), b);
		  	resp.put("response", message);
	  		response.getWriter().write(resp.toString());
	  }
	  
	  
	
}

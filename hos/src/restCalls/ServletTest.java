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
import hos.BucketInfo;
import hos.HadoopObjectStore;
import hos.UserId;

public class ServletTest extends HttpServlet{

	  /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	HbsUtil util = new HbsUtil();
	boolean folderAlreadyExists; 
	
	  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
	  	JSONObject resp = new JSONObject();

	  	String message = String.valueOf(System.currentTimeMillis());
	  	try{
	  		Thread.sleep(10000);
	  	} catch (InterruptedException e){
	  		e.printStackTrace();
	  	}
	  	
	  	message += " - " + String.valueOf(System.currentTimeMillis());
	  	resp.put("response", message);
  		response.getWriter().write(resp.toString());	  		
	  }
	
}

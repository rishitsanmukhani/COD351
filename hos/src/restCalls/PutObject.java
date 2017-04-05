package restCalls;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import javax.el.MapELResolver;
import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;

import hbs.HbsUtil;
import hos.BucketId;
import hos.HadoopObjectStore;
import hos.ObjectInfo;
import hos.ObjectProtocol;

public class PutObject extends HttpServlet{
	
	HbsUtil util = new HbsUtil();
	
		public void doPut(HttpServletRequest request, HttpServletResponse response) {
			JSONObject resp = new JSONObject();
			String bucketKey = (String) request.getParameter("bucketKey");
			String objectKey = (String) request.getParameter("objectKey");
			String charEncoding = request.getCharacterEncoding();
			String contentType = request.getContentType();
			int lenOfFile = request.getContentLength();
			Map<String,String[]> umetadata = new HashMap<String, String[]>(request.getParameterMap());
			umetadata.remove("bucketKey");
			umetadata.remove("objectKey");
			
			ObjectInfo objectInfo = new ObjectInfo(null,charEncoding,contentType,objectKey,lenOfFile,umetadata);		
			String responseMessage = "";
			try {
				ServletInputStream fsIn = request.getInputStream();
				responseMessage += HadoopObjectStore.getHadoopObjectStore().PutObject(new BucketId(bucketKey), objectInfo, fsIn);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				responseMessage += e1.getMessage();
				e1.printStackTrace();
			}
			
			resp.put("response", responseMessage);
			try{
				response.getWriter().write(resp.toString());				
			} catch (Exception e){
				e.printStackTrace();
			}
		}	  
}

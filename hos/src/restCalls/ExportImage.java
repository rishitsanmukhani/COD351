package restCalls;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import hbs.HbsUtil;
import hos.BucketId;
import hos.HadoopObjectStore;
import hos.ObjectId;
import hos.ObjectInfo;
import hos.ObjectOut;

public class ExportImage extends HttpServlet{

	  HbsUtil util = new HbsUtil();
		
	  public void doGet(HttpServletRequest request, HttpServletResponse response)
			  throws ServletException, IOException {
			String imageKey = (String) request.getParameter("imageKey");

			ExportImage export = new ExportImage();
			ImagaOut imgOutput = export.exportImage(new ImageId(imageKey));

	  		ImageInfo imgInfo = imgOutput.imageInfo;
	  		DataInputStream din = imgOutput.dis;
	  		
			response.setContentLength(imgInfo.lenofFile);

		  	response.setHeader("Content-Disposition","imagename=\"" + imgInfo.key + "\"");//fileName);
			ServletOutputStream sos = response.getOutputStream();

			byte[] buffer = new byte[10240];
			for (int length = 0; (length = din.read(buffer)) > 0; ) {
				sos.write(buffer, 0, length);
			}

			sos.close();
			din.close();
			
	  }
}

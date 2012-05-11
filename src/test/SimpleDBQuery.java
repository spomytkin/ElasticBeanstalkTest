package test;



import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;

@WebServlet(description = "SimpleDBQuery from test", urlPatterns = { "/SimpleDBQuery" })
public class SimpleDBQuery extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static final Logger log= Logger.getLogger("SimpleDBQuery");
	private static final String tableName = "Test";
	 AmazonDynamoDBClient dynamoDB;
//	 AmazonS3 s3;
	 AmazonSimpleDB sdb ;
    /**
     * @see HttpServlet#HttpServlet()
     */
    public SimpleDBQuery() {
        super();
       
        
        AWSCredentials credentials = null;
		try {
			credentials = new PropertiesCredentials(
					SimpleDBQuery.class.getResourceAsStream("AwsCredentials.properties"));
		} catch (IOException e) {
			log.severe("Failed to get credentials"+e);
			e.printStackTrace();
		}

	//	s3 = new AmazonS3Client(credentials);
    //    dynamoDB = new AmazonDynamoDBClient(credentials);
        sdb = new AmazonSimpleDBClient(credentials);
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		PrintWriter pw = response.getWriter();

		try{
		response.setContentType("text/plain");


				String table = "Test";
				String where = "";
				
				if(request.getParameter("where")!=null)where=request.getParameter("where");
				if(request.getParameter("table")!=null)where=request.getParameter("table");
				
				 pw.println("starting...");

				  try{
					// Select data from a domain
			        // Notice the use of backticks around the domain name in our select expression.
			        String selectExpression = "select * from `" + table + "`  "+where;
			        pw.println("Selecting: " + selectExpression + "\n");
			        long start = System.currentTimeMillis();
			        SelectRequest selectRequest = new SelectRequest(selectExpression);
			        pw.println("  selectRequest "+(System.currentTimeMillis()-start));
			        SelectResult rez = sdb.select(selectRequest);
			        pw.println("  SelectResult "+(System.currentTimeMillis()-start));
			        List<Item> items = rez.getItems();
			        pw.println("  getItems "+(System.currentTimeMillis()-start));
			        for (Item item : items) {
			            pw.println("  Item");
			            pw.println("    Name: " + item.getName());
			            
			            for (Attribute attribute : item.getAttributes()) {
			                pw.println("      Attribute");
			                pw.println("        Name:  " + attribute.getName());
			                pw.println("        Value: " + attribute.getValue());
			              
			                
			            }
			            
			        }
			        pw.println("  done in  "+(System.currentTimeMillis()-start));
			        pw.println();
			    } catch (AmazonServiceException ase) {
			        pw.println("Caught an AmazonServiceException, which means your request made it "
			                + "to Amazon SimpleDB, but was rejected with an error response for some reason.");
			        pw.println("Error Message:    " + ase.getMessage());
			        pw.println("HTTP Status Code: " + ase.getStatusCode());
			        pw.println("AWS Error Code:   " + ase.getErrorCode());
			        pw.println("Error Type:       " + ase.getErrorType());
			        pw.println("Request ID:       " + ase.getRequestId());
			    } catch (AmazonClientException ace) {
			        pw.println("Caught an AmazonClientException, which means the client encountered "
			                + "a serious internal problem while trying to communicate with SimpleDB, "
			                + "such as not being able to access the network.");
			        pw.println("Error Message: " + ace.getMessage());
			    }

					
				

				 pw.println("done.");
		}catch(Exception e){
					pw.println("failed with Exception"+e);
					e.printStackTrace(pw);
					log.warning("failed with Exception"+e);
				}

	}



	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request,response);
	}



	
}

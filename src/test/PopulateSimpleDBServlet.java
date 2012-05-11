package test;




import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;



@WebServlet(description = "Generates Test content toSimpleDB", urlPatterns = { "/PopulateSimpleDBServlet" })
public class PopulateSimpleDBServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static final Logger log= Logger.getLogger("PopulateSimpleDBServlet");
	/*
		10 of length 40 
		20 of length 20
		15 of length 5
		5 of length 250
	 */
	static final int l40 =10;
	static final int l20 =20;
	static final int l5 =11;
	static final int l250 =5;
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		PrintWriter pw = response.getWriter();

		try{
		response.setContentType("text/plain");
		
        AmazonSimpleDB sdb = new AmazonSimpleDBClient(new PropertiesCredentials(
        		PopulateSimpleDBServlet.class.getResourceAsStream("AwsCredentials.properties")));
        String myDomain = "Test";
        pw.println("===========================================");
        pw.println("    populate SimpleDB Domain "+myDomain+" with Test data");
        pw.println("===========================================\n");
        log.info("    populate SimpleDB Domain "+myDomain+" with Test data");
        System.out.println("    populate SimpleDB Domain "+myDomain+" with Test data");
        try {
   /*         // Create a domain

            pw.println("Creating domain called " + myDomain + ".\n");
            sdb.createDomain(new CreateDomainRequest(myDomain));

            // List domains
            pw.println("Listing all domains in your account:\n");
            for (String domainName : sdb.listDomains().getDomainNames()) {
                pw.println("  " + domainName);
            }
          
            */
            
            // Put data into a domain
        	Date tday = new Date();
        	pw.println(tday+" Putting data into " + myDomain + " domain.\n");
        	log.info(tday+" Putting data into " + myDomain + " domain.\n");
            System.out.println(tday+" Putting data into " + myDomain + " domain.\n");
            
            pw.println("Record configuration:\n");
            pw.println(l40 + " of length 40 .\n");
            pw.println(l20 + " of length 20 .\n");
            pw.println(l5 + " of length 5 .\n");
            pw.println(l250 + " of length 250 .\n");
            
            long from=400001;            
			long batchPutNum=25;
			long reportEveryBatch =10;//250
			long numReports =400;
		try{	
			from=Long.parseLong(request.getParameter("from"));
			batchPutNum=Integer.parseInt(request.getParameter("batchPutNum"));
			reportEveryBatch=Integer.parseInt(request.getParameter("reportEveryBatch"));
			numReports=Integer.parseInt(request.getParameter("numReports"));
		}catch(Exception ie ){
			pw.println("failed with Exception"+ie);
			ie.printStackTrace(pw);
		}
			
			long total = reportEveryBatch*batchPutNum*numReports;//100K
            pw.println("Starting from key " +from + ".\n");
        	log.info("Starting from key " +from + ".\n");
            System.out.println("Starting from key " +from + ".\n");
            
            
			pw.println("Putting total" + total + " reporting reportEvery "+reportEveryBatch+" batchPutNum "+batchPutNum+ ".\n");
			/* batchPutAttributes limits:
				256 attribute name-value pairs per item
				1 MB request size
				1 billion attributes per domain
				10 GB of total user data storage per domain
				25 item limit per BatchPutAttributes operation
			 */
			long start = System.currentTimeMillis();  
			for(int rep=0;rep<numReports;rep++){
				long startRep = System.currentTimeMillis();  
				for(int btc=0;btc<reportEveryBatch;btc++){
					sdb.batchPutAttributes(new BatchPutAttributesRequest(myDomain, createSampleData(from, batchPutNum)));
					from+=batchPutNum;
				}	
				long btchRepTime = System.currentTimeMillis()-startRep;
	            pw.println("Done: "+from+" records in:"+(System.currentTimeMillis()-start)+"ms.\n");
	        	log.info("Done: "+from+" records in:"+(System.currentTimeMillis()-start)+"ms.\n");
	            System.out.println("Done: "+from+" records in:"+(System.currentTimeMillis()-start)+"ms.\n");
	            
				double rps= (reportEveryBatch*batchPutNum*1000)/btchRepTime;
	            pw.println("set: "+rep+" done in:"+btchRepTime+"ms, "+rps+" records per s.\n");		
	        	log.info("set: "+rep+" done in:"+btchRepTime+"ms, "+rps+" records per s.\n");		
	            System.out.println("set: "+rep+" done in:"+btchRepTime+"ms, "+rps+" records per s.\n");		
	            
			}


            pw.println("Done");

     

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
		}catch(Exception e){
			pw.println("failed with Exception"+e);
			e.printStackTrace(pw);
			log.warning("failed with Exception"+e);
		}
    }

    /**
     * Creates an array of SimpleDB ReplaceableItems populated with sample data.
     * index key named from start
     * @return An array of sample item data.
     */
    private static List<ReplaceableItem> createSampleData(long start, long num) {
        List<ReplaceableItem> sampleData = new ArrayList<ReplaceableItem>();

        for(long i = 0;i<num;i++ ){
        	ReplaceableItem ra = new ReplaceableItem(""+(start+i));
        	Collection<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();

        	for(int cnt=0;cnt<l40;cnt++){
        		attributes.add( new ReplaceableAttribute("l40-"+cnt, "0123456789012345678901234567890123456789", true));
        	}
        	for(int cnt=0;cnt<l20;cnt++){
        		attributes.add( new ReplaceableAttribute("l20-"+cnt, "01234567890123456789", true));
        	}
        	for(int cnt=0;cnt<l5;cnt++){
        		attributes.add( new ReplaceableAttribute("l5-"+cnt, "01234", true));
        	}
        	for(int cnt=0;cnt<l250;cnt++){
        		attributes.add( new ReplaceableAttribute("l250-"+cnt, "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789", true));
        	}
        	
			ra.setAttributes(attributes); 
            sampleData.add(ra);
        	
        }

        return sampleData;
    }
}

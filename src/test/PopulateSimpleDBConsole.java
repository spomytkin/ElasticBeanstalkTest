package test;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;


public class PopulateSimpleDBConsole {
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
    public static void main(String[] args) throws Exception {
        /*
         * Important: Be sure to fill in your AWS access credentials in the
         *            AwsCredentials.properties file before you try to run this
         *            sample.
         * http://aws.amazon.com/security-credentials
         */
        AmazonSimpleDB sdb = new AmazonSimpleDBClient(new PropertiesCredentials(
        		PopulateSimpleDBConsole.class.getResourceAsStream("AwsCredentials.properties")));

        System.out.println("===========================================");
        System.out.println("    populate SimpleDB with Test data set 2 con-2");
        System.out.println("===========================================\n");

        try {
            // Create a domain
            String myDomain = "Test";
            System.out.println("Creating domain called " + myDomain + ".\n");
            sdb.createDomain(new CreateDomainRequest(myDomain));

            // List domains
            System.out.println("Listing all domains in your account:\n");
            for (String domainName : sdb.listDomains().getDomainNames()) {
                System.out.println("  " + domainName);
            }
          
            Date tday = new Date();
            
            // Put data into a domain
            System.out.println(tday+ " Putting data into " + myDomain + " domain.\n");
            
            System.out.println("Record configuration:\n");
            System.out.println(l40 + " of length 40 .\n");
            System.out.println(l20 + " of length 20 .\n");
            System.out.println(l5 + " of length 5 .\n");
            System.out.println(l250 + " of length 250 .\n");
            
            long from=500001;            
			long batchPutNum=25;
			long reportEveryBatch =100;//25000
			long numReports =40;
			long total = reportEveryBatch*batchPutNum*numReports;//100K
            System.out.println("Starting from key " +from + ".\n");

			System.out.println("Putting total" + total + " reporting reportEvery "+reportEveryBatch+" batchPutNum "+batchPutNum+ ".\n");
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
	            System.out.println("Done: "+from+" records in:"+(System.currentTimeMillis()-start)+"ms.\n");
				double rps= (reportEveryBatch*batchPutNum*1000)/btchRepTime;
	            System.out.println("set: "+rep+" done in:"+btchRepTime+"ms, "+rps+" records per s.\n");				
			}


            System.out.println("Done");

     

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon SimpleDB, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with SimpleDB, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
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

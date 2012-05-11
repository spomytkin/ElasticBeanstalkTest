package test;



import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.services.dynamodb.model.TableStatus;


public class PopulateDynamoDBConsole {
	/*
		10 of length 40 
		20 of length 20
		15 of length 5
		5 of length 250
	 */
	static final int l40 =10;
	static final int l20 =20;
	static final int l5 =15;
	static final int l250 =5;
	private static final String tableName = "Test";
	
    static AmazonDynamoDBClient dynamoDB;

    /**
     * The only information needed to create a client are security credentials
     * consisting of the AWS Access Key ID and Secret Access Key. All other
     * configuration, such as the service endpoints, are performed
     * automatically. Client parameters, such as proxies, can be specified in an
     * optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.PropertiesCredentials
     * @see com.amazonaws.ClientConfiguration
     */
    private static void init() throws Exception {
        AWSCredentials credentials = new PropertiesCredentials(
        		PopulateDynamoDBConsole.class.getResourceAsStream("AwsCredentials.properties"));

        dynamoDB = new AmazonDynamoDBClient(credentials);
    }
    private static void waitForTableToBecomeAvailable(String tableName) {
        System.out.println("Waiting for " + tableName + " to become ACTIVE...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {Thread.sleep(1000 * 20);} catch (Exception e) {}
            try {
                DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
                TableDescription tableDescription = dynamoDB.describeTable(request).getTable();
                String tableStatus = tableDescription.getTableStatus();
                System.out.println("  - current state: " + tableStatus);
                if (tableStatus.equals(TableStatus.ACTIVE.toString())) return;
            } catch (AmazonServiceException ase) {
                if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException") == false) throw ase;
            }
        }

        throw new RuntimeException("Table " + tableName + " never went active");
    }
    
    public static void main(String[] args) throws Exception {
    	init(); 
    /*    // Create a table with a primary key named 'name', which holds a string
        CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
            .withKeySchema(new KeySchema(new KeySchemaElement().withAttributeName("Id").withAttributeType("N")))
            .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L));
        TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
        System.out.println("Created Table: " + createdTableDescription);

        // Wait for it to become active
        waitForTableToBecomeAvailable(tableName);
*/
        // Describe our new table
        DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
        TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
        System.out.println("Table Description: " + tableDescription);



        System.out.println("===========================================");
        System.out.println("    populate DynamoDB with Test data set 3");
        System.out.println("===========================================\n");

        try {


          
            Date tday = new Date();
            
            // Put data into a domain
            System.out.println(tday+ " Putting data into " + tableName + " table.\n");
            
            System.out.println("Record configuration:\n");
            System.out.println(l40 + " of length 40 .\n");
            System.out.println(l20 + " of length 20 .\n");
            System.out.println(l5 + " of length 5 .\n");
            System.out.println(l250 + " of length 250 .\n");
            
            long from=400001;            
			long batchPutNum=1;
			long reportEveryBatch =2500;
			long numReports =40;
			long total = reportEveryBatch*batchPutNum*numReports;//100K
            System.out.println("Starting from key " +from + ".\n");

			System.out.println("Putting total " + total + " reporting reportEvery "+reportEveryBatch+" batchPutNum "+batchPutNum+ ".\n");
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
		            Map<String, AttributeValue> item = newItem(from);
		            PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
		            PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
					from+=batchPutNum;
				}	
				long btchRepTime = System.currentTimeMillis()-startRep;
	            System.out.println("Done: "+from+" record in:"+(System.currentTimeMillis()-start)+"ms.\n");
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

    private static Map<String, AttributeValue> newItem(long i) {

        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        	item.put("Id", new AttributeValue().withN(""+i));
        	for(int cnt=0;cnt<l40;cnt++){
        		item.put("l40-"+cnt,  new AttributeValue("0123456789012345678901234567890123456789"));
        	}
        	for(int cnt=0;cnt<l20;cnt++){
        		item.put( "l20-"+cnt,  new AttributeValue("01234567890123456789"));
        	}
        	for(int cnt=0;cnt<l5;cnt++){
        		item.put( "l5-"+cnt, new AttributeValue( "01234"));
        	}
        	for(int cnt=0;cnt<l250;cnt++){
        		item.put( "l250-"+cnt, new AttributeValue( "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"));
        	}
        	
			return item;

    }
}

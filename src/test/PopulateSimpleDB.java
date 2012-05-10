package test;


import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;
import com.amazonaws.services.simpledb.model.SelectRequest;


public class PopulateSimpleDB {

    public static void main(String[] args) throws Exception {
        /*
         * Important: Be sure to fill in your AWS access credentials in the
         *            AwsCredentials.properties file before you try to run this
         *            sample.
         * http://aws.amazon.com/security-credentials
         */
        AmazonSimpleDB sdb = new AmazonSimpleDBClient(new PropertiesCredentials(
                PopulateSimpleDB.class.getResourceAsStream("AwsCredentials.properties")));

        System.out.println("===========================================");
        System.out.println("    populate SimpleDB with Ticker data");
        System.out.println("===========================================\n");

        try {
            // Create a domain
            String myDomain = "Ticker";
            System.out.println("Creating domain called " + myDomain + ".\n");
            sdb.createDomain(new CreateDomainRequest(myDomain));

            // List domains
            System.out.println("Listing all domains in your account:\n");
            for (String domainName : sdb.listDomains().getDomainNames()) {
                System.out.println("  " + domainName);
            }
            System.out.println();

            // Put data into a domain
            System.out.println("Putting data into " + myDomain + " domain.\n");
            sdb.batchPutAttributes(new BatchPutAttributesRequest(myDomain, createSampleData()));

            // Select data from a domain
            // Notice the use of backticks around the domain name in our select expression.
            String selectExpression = "select * from `" + myDomain + "` where Change_Date = '3-3-2012'";
            System.out.println("Selecting: " + selectExpression + "\n");
            SelectRequest selectRequest = new SelectRequest(selectExpression);
            for (Item item : sdb.select(selectRequest).getItems()) {
                System.out.println("  Item");
                System.out.println("    Name: " + item.getName());
                for (Attribute attribute : item.getAttributes()) {
                    System.out.println("      Attribute");
                    System.out.println("        Name:  " + attribute.getName());
                    System.out.println("        Value: " + attribute.getValue());
                }
            }
            System.out.println();

     

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
     *
     * @return An array of sample item data.
     */
    private static List<ReplaceableItem> createSampleData() {
        List<ReplaceableItem> sampleData = new ArrayList<ReplaceableItem>();

        sampleData.add(new ReplaceableItem("Item_05").withAttributes(
                new ReplaceableAttribute("Name", "Item_01", true),
                new ReplaceableAttribute("Price", "6460.19", true),
                new ReplaceableAttribute("Number", "132", true),
                new ReplaceableAttribute("Change Time", "20:13:54", true),
                new ReplaceableAttribute("Change_Date", "3-3-2012", true)));

        sampleData.add(new ReplaceableItem("Item_06").withAttributes(
                new ReplaceableAttribute("Name", "Item_02", true),
                new ReplaceableAttribute("Price", "1212", true),
                new ReplaceableAttribute("Number", "11", true),
                new ReplaceableAttribute("Change Time", "20:13:55", true),
                new ReplaceableAttribute("Change_Date", "3-3-2012", true)));



        return sampleData;
    }
}

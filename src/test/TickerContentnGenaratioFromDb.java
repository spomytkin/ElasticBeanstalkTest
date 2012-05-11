package test;



import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.SelectRequest;

@WebServlet(description = "Generates Test content to S3 Bucket", urlPatterns = { "/TickerContentGenarationFromDb" })
public class TickerContentnGenaratioFromDb extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static final Logger log= Logger.getLogger("TickerContentGenaration");
	private static final String tableName = "Ticker";
	 AmazonDynamoDBClient dynamoDB;
	 AmazonS3 s3;
	 AmazonSimpleDB sdb ;
    /**
     * @see HttpServlet#HttpServlet()
     */
    public TickerContentnGenaratioFromDb() {
        super();
       
        
        AWSCredentials credentials = null;
		try {
			credentials = new PropertiesCredentials(
					TickerContentnGenaratioFromDb.class.getResourceAsStream("AwsCredentials.properties"));
		} catch (IOException e) {
			log.severe("Failed to get credentials"+e);
			e.printStackTrace();
		}

		s3 = new AmazonS3Client(credentials);
        dynamoDB = new AmazonDynamoDBClient(credentials);
        sdb = new AmazonSimpleDBClient(credentials);
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		PrintWriter pw = response.getWriter();

		try{
		response.setContentType("text/plain");
		 File dir = new File(System.getProperty("java.io.tmpdir"));
			
		 
				if(true) {					 
					  File testFile = new File(dir, System.currentTimeMillis() + ".txt");
					  pw.println("CanRead: " + dir.canRead() + ", CanWrite: " + dir.canWrite() + ", CreatedNewFile: " + testFile.createNewFile());
					}

	
				
				String bucketName = "TickerContent";				
				String fileName = "tickerContent.html";
				File file = new File(dir,fileName);
				

				 pw.println("starting...");


					ArrayList<TickerRow> tickerRowList = getRowList(pw);
					
					writeFile(file, tickerRowList);
					
					PutObjectRequest prequest = new PutObjectRequest(bucketName, fileName,
							file);
					prequest.setCannedAcl(CannedAccessControlList.PublicRead);
					s3.putObject(prequest);				
				

				 pw.println("done.");
		}catch(Exception e){
					pw.println("failed with Exception"+e);
					e.printStackTrace(pw);
					log.warning("failed with Exception"+e);
				}

	}
	

private ArrayList<TickerRow> getRowList(PrintWriter pw) {
	 ArrayList<TickerRow> rl = new ArrayList<TickerRow>();
	 // Scan items for movies with a year attribute greater than 1985
 /*   HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
    Condition condition = new Condition()
        .withComparisonOperator(ComparisonOperator.GT.toString())
        .withAttributeValueList(new AttributeValue().withN("1985"));
    scanFilter.put("year", condition);
    ScanRequest scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
    ScanResult scanResult = dynamoDB.scan(scanRequest);
    scanResult.getScannedCount();
		return null;*/
	
	
    try {
        // Create a domain
        String myDomain = "Ticker";


        // List domains
        pw.println("Listing all domains in your account:\n");
        for (String domainName : sdb.listDomains().getDomainNames()) {
            pw.println("  " + domainName);
        }
        pw.println();


       
        // Select data from a domain
        // Notice the use of backticks around the domain name in our select expression.
        String selectExpression = "select * from `" + myDomain + "` where Change_Date = '3-3-2012'";
        pw.println("Selecting: " + selectExpression + "\n");
        SelectRequest selectRequest = new SelectRequest(selectExpression);
        for (Item item : sdb.select(selectRequest).getItems()) {
            pw.println("  Item");
            pw.println("    Name: " + item.getName());
            TickerRow tr = new TickerRow();
            
            for (Attribute attribute : item.getAttributes()) {
                pw.println("      Attribute");
                pw.println("        Name:  " + attribute.getName());
                pw.println("        Value: " + attribute.getValue());
                if (attribute.getName().equals("Name")){
                	tr.setName( attribute.getValue()+" ");
                	}else if(attribute.getName().equals("Price")){
                    	tr.setPrice(attribute.getValue()+" ");
                    	}else if(attribute.getName().equals("Number")){
                        	tr.setNumber(attribute.getValue()+" ");
                       }else if(attribute.getName().equals("Change Time")){
                       	tr.setTime(attribute.getValue()+" ");
                      }else if(attribute.getName().equals("Change_Date")){
                         	tr.setDate(attribute.getValue()+" ");
                        }else if(attribute.getName().equals("rowUpdated")){
                         	tr.setRowUpdated(true);
                        }
                
                
            }
            
            rl.add(tr);
            
        }
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
	return rl;
	}


private static class TickerRow {
		
		String rowNumber;
		String name;
		String price;
		boolean priceChanged = false;
		public String getName() {
			return name;
		}


		public void setName(String name) {
			this.name = name;
		}


		public String getPrice() {
			return price;
		}


		public void setPrice(String price) {
			this.price = price;
		}


		public String getNumber() {
			return number;
		}


		public void setNumber(String number) {
			this.number = number;
		}


		public String getTime() {
			return time;
		}


		public void setTime(String time) {
			this.time = time;
		}


		public String getDate() {
			return date;
		}


		public void setDate(String date) {
			this.date = date;
		}


		String number;
		boolean numberChanged = false;
		String time;
		boolean timeChanged = false;
		String date;
		boolean dateChanged = false;
		public boolean rowUpdated;
		
		public boolean isPriceChanged() {
			return priceChanged;
		}


		public void setPriceChanged(boolean priceChanged) {
			this.priceChanged = priceChanged;
		}


		public boolean isNumberChanged() {
			return numberChanged;
		}


		public void setNumberChanged(boolean numberChanged) {
			this.numberChanged = numberChanged;
		}


		public boolean isDateChanged() {
			return dateChanged;
		}


		public void setDateChanged(boolean dateChanged) {
			this.dateChanged = dateChanged;
		}


		public boolean isRowUpdated() {
			return rowUpdated;
		}


		public void setRowUpdated(boolean rowUpdated) {
			this.rowUpdated = rowUpdated;
		}


		public TickerRow() {
			super();
			
			this.name = "Item " + this.rowNumber;

		}

		
		public String rowString() {
			StringBuilder sb = new StringBuilder();
			sb.append("   <tr>");
			
			String rowTD = "<td>";
			if ( this.rowUpdated ) {
				rowTD = "<td class='tickerUpdatedRow'>";
			}
			
			sb.append(rowTD); 
			sb.append(this.name);
			sb.append("</td>");
			
			if (this.priceChanged) {
				sb.append("<td class='tickerNewValue'>");
			} else {
				sb.append(rowTD);
			}
			sb.append(this.price);
			sb.append("</td>");
			
			if (this.numberChanged) {
				sb.append("<td class='tickerNewValue'>");
			} else {
				sb.append(rowTD);
			}
			sb.append(this.number);
			sb.append("</td>");
			
			if (this.timeChanged) {
				sb.append("<td class='tickerNewValue'>");
			} else {
				sb.append(rowTD);
			}
			sb.append(this.time); 
			sb.append("</td>");
			
			if (this.dateChanged) {
				sb.append("<td class='tickerNewValue'>");
			} else {
				sb.append(rowTD);
			}
			sb.append(this.date); 
			sb.append("</td>");
			
			sb.append("</tr>");
			
			return sb.toString();
		}
	}
	
			
	private static void writeFile(File file,
			ArrayList<TickerRow> tickerRowList) {
		try {
			// Create file 
			FileWriter fstream = new FileWriter(file);
			BufferedWriter out = new BufferedWriter(fstream);
			
			writeOutput(out, tickerRowList);
			
			// Close the output stream
			out.close();
		} catch (Exception e){ 
			System.err.println("Error: " + e.getMessage());
		}
	}

	private static void writeOutput(
			BufferedWriter out, 
			ArrayList<TickerRow> tickerRowList
			) throws IOException {
		
		String date = (new Date()).toString();
		
		out.write("<table id='tickerTable' border='0' cellpadding='0' cellspacing='0'>\n");
		out.write("<caption align='top'>Last update:" +date +  "</caption>  where Change_Date = '3-3-2012'\n");
		out.write("<thead>\n");
		out.write("   <th>Name</th>\n");
		out.write("   <th>Price</th>\n");
		out.write("   <th>Number</th>\n");
		out.write("   <th>Change Time</th>\n");
		out.write("   <th>Change Date</th>\n");
		out.write("</thead>\n");
		out.write("<tbody>\n");

		for ( TickerRow tickerRow : tickerRowList ) {
			out.write(tickerRow.rowString() + "\n");
		}
		
		out.write("</tbody>\n");
		out.write("</table>\n");
	}

	

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request,response);
	}



	
}

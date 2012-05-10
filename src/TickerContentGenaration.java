


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;

/**
 * Servlet implementation class BucketsListing
 */
@WebServlet(description = "Generates Test content to S3 Bucket", urlPatterns = { "/TickerContentGenaration" })
public class TickerContentGenaration extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static final Logger log= Logger.getLogger("TickerContentGenaration");
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public TickerContentGenaration() {
        super();
        // TODO Auto-generated constructor stub
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

				// Number of rows in each file.
				int NUMBER_TICKER_ROWS = 15;

				// Number of files made.
				int NUMBER_CONTENT_FILES = 20;
				
				// Try to modify one line out of N when modifying the file.
				int MODIFY_NTH_ROW = 6;				
				
				String bucketName = "TickerContent";
				
				try{
					NUMBER_TICKER_ROWS=Integer.parseInt(request.getParameter("NUMBER_TICKER_ROWS"));
					NUMBER_CONTENT_FILES=Integer.parseInt(request.getParameter("NUMBER_CONTENT_FILES"));
					MODIFY_NTH_ROW=Integer.parseInt(request.getParameter("MODIFY_NTH_ROW"));
					bucketName=request.getParameter("bucketName");
				}catch(Exception ie ){
					pw.println("failed with Exception"+ie);
					ie.printStackTrace(pw);
				}
				
				AmazonS3 s3 = new AmazonS3Client(new PropertiesCredentials(
						TickerContentGenaration.class.getResourceAsStream("AwsCredentials.properties")));
				 pw.println("starting...");
				
				// Create ticker table.
				ArrayList<TickerRow> tickerRowList = new ArrayList<TickerRow>();
				for (int i = 1; i <= NUMBER_TICKER_ROWS; i++ ) {
					TickerRow tickerRow = new TickerRow(i);
					tickerRowList.add(tickerRow);
				}
				 pw.println("will generate "+NUMBER_CONTENT_FILES+" files at "+dir.getAbsolutePath());
				 
				for (int i = 0; i <= NUMBER_CONTENT_FILES; i++ ) {
					if ( i > 1 ) {
						modifyTickerRows(tickerRowList,MODIFY_NTH_ROW);
					}
					String fileName = "tickerContent" + (100+i) + ".html";
					pw.println("writeFile "+i+"  " +fileName);
					
					File file=new File(dir,fileName);
					writeFile(file, tickerRowList, i);
					
					PutObjectRequest prequest = new PutObjectRequest(bucketName, fileName,
							file);
					prequest.setCannedAcl(CannedAccessControlList.PublicRead);
					s3.putObject(prequest);
				}
				

				 pw.println("done.");
		}catch(Exception e){
					pw.println("failed with Exception"+e);
					e.printStackTrace(pw);
					log.warning("failed with Exception"+e);
				}

	}
	
	private static void modifyTickerRows(ArrayList<TickerRow> tickerRowList, int MODIFY_NTH_ROW) {
		if ( tickerRowList != null ) {
			// First clear any previous changed values.
			for ( TickerRow tickerRow : tickerRowList ) {
				tickerRow.rowUpdated = false;
				tickerRow.priceChanged = false;
				tickerRow.numberChanged = false;
				tickerRow.timeChanged = false;
				tickerRow.dateChanged = false;
			}
			
			// Now decide what to modify - we will try to modify one line out of N
			for ( TickerRow tickerRow : tickerRowList ) {
				if ( OneOutOfNTrue(MODIFY_NTH_ROW) ) {
					// Modify this row.  Now decide what to modify
					tickerRow.rowUpdated = true;
					int r = randomInt(1, 3);
					if ( r == 1 ) {
						tickerRow.priceChanged = true;
						tickerRow.makePriceValue();
					} else if ( r == 2 ) {
						tickerRow.numberChanged = true;
						tickerRow.makeNumberValue();
					} else if ( r == 3 ) {
						tickerRow.timeChanged = true;
						tickerRow.makeTimeValue();
						tickerRow.dateChanged = true;
						tickerRow.makeDateValue();
					}
				}
			}
			

		
		}
		
	}

	private static boolean OneOutOfNTrue(int n) {
		boolean b = false;
		int r = randomInt(1, n);
		if ( r == n ) {
			b = true;
		}
		return b;
	}
			
			
	private static void writeFile(File file,
			ArrayList<TickerRow> tickerRowList,
			int index) {
		try {
			// Create file 
			FileWriter fstream = new FileWriter(file);
			BufferedWriter out = new BufferedWriter(fstream);
			
			writeOutput(out, tickerRowList, index);
			
			// Close the output stream
			out.close();
		} catch (Exception e){ 
			System.err.println("Error: " + e.getMessage());
		}
	}

	private static void writeOutput(
			BufferedWriter out, 
			ArrayList<TickerRow> tickerRowList,
			int index) throws IOException {
		
		String date = (new Date()).toString();
		
		out.write("<table id='tickerTable' border='0' cellpadding='0' cellspacing='0'>\n");
		out.write("<caption align='top'>Last update:" +date +  "</caption>\n");
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

	
	public static int randomInt(int min, int max) {
		Random random = new Random(); 
		int randomNum = random.nextInt(max - min + 1) + min; 
		return randomNum;
	}
	
	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request,response);
	}

private static class TickerRow {
		
		int rowNumber;
		String name;
		String price;
		boolean priceChanged = false;
		String number;
		boolean numberChanged = false;
		String time;
		boolean timeChanged = false;
		String date;
		boolean dateChanged = false;
		public boolean rowUpdated;
		
		public TickerRow(int thisRowNumber) {
			super();
			this.rowNumber = thisRowNumber;
			this.name = "Item " + this.rowNumber;
			makePriceValue();
			makeNumberValue();
			makeTimeValue();
			makeDateValue();
		}

		public void makeDateValue() {
			this.date =  "" + randomInt(1, 5) + "-" + randomInt(1, 28) + "-2012";
		}

		public void makeTimeValue() {
			this.time =  "" + randomInt(1, 24) + ":" + randomInt(10, 59) + ":" + randomInt(10, 59);
		}

		public void makeNumberValue() {
			this.number =  "" + randomInt(1000, 20000);
		}

		public void makePriceValue() {
			this.price = "" + randomInt(100, 10000) + "." + randomInt(0, 99);
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
	
}

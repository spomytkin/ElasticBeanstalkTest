package test;


import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;

/**
 * Servlet implementation class BucketsListing
 */
@WebServlet(description = "call S3 to List Buckets", urlPatterns = { "/BucketsListing" })
public class BucketsListing extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public BucketsListing() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		AmazonS3 s3 = new AmazonS3Client(new PropertiesCredentials(

				BucketsListing.class.getResourceAsStream("AwsCredentials.properties")));
				PrintWriter pw = response.getWriter();
				response.setContentType("text/plain");
				pw.println("===========================================\n");
				pw.println("Test connectivity with Amazon S3\n");
				pw.println("===========================================\n");

				try {

					/*
					 * List the buckets in your account
					 */
					pw.println("Listing buckets");
					for (Bucket bucket : s3.listBuckets()) {
						pw.println("\n - " + bucket.getName());
					}
					pw.println();
				} catch (AmazonServiceException ase) {
					System.out
							.println("Caught an AmazonServiceException, whichmeans your request made it "
									+ "to Amazon S3, but was rejected with an errorresponse for some reason.");
					pw.println("Error Message:    " + ase.getMessage());
					pw.println("HTTP Status Code: " + ase.getStatusCode());
					pw.println("AWS Error Code:   " + ase.getErrorCode());
					pw.println("Error Type:       " + ase.getErrorType());
					pw.println("Request ID:       " + ase.getRequestId());
				} catch (AmazonClientException ace) {
					pw.println("Caught an AmazonClientException, whichmeans the client encountered "
									+ "a serious internal problem while trying tocommunicate with S3, "
									+ "such as not being able to access the network.");
					pw.println("Error Message: " + ace.getMessage());
				}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request,response);
	}

}

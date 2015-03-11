package com.aws.emr.play;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Properties;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Test {

	/**
     * Creates a temporary file with text data to demonstrate uploading a file
     * to Amazon S3
     *
     * @return A newly created temporary file with text data.
     *
     * @throws IOException
     */
    private File createSampleFile() throws IOException {
        File file = File.createTempFile("aws-java-sdk-", ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.write("01234567890112345678901234\n");
        writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
        writer.write("01234567890112345678901234\n");
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.close();

        return file;
    }

    
    private File createSampleFile1(String pref) throws IOException {
    	String fileName = pref + "aws-java-sdk-";
        File file = File.createTempFile(fileName, ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.write("01234567890112345678901234\n");
        writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
        writer.write("01234567890112345678901234\n");
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.close();

        return file;
    }
    
    /**
     * Displays the contents of the specified input stream as text.
     *
     * @param input
     *            The input stream to display as text.
     *
     * @throws IOException
     */
    private void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }		
	
    public void testFileListing() throws IOException{
    	
    	//filterbucket7b7532a5-2afb-4762-aa80-98732487aae5
    	
    	Properties properties = new Properties();
		//String awsKeyFile = System.getenv("AWS_CREDENTIAL_FILE");
        //properties.load(new FileInputStream(awsKeyFile));
		properties.load(new FileInputStream("keys/myCredentialFile"));
        		
        AWSCredentials creds = new BasicAWSCredentials(properties.getProperty("AWSAccessKeyId"),
				properties.getProperty("AWSSecretKey"));

        AmazonS3 s3 = new AmazonS3Client(creds);
        Region usWest1 = Region.getRegion(Regions.US_WEST_1);
        s3.setRegion(usWest1);
        
        String bucketName = "filterbucket7b7532a5-2afb-4762-aa80-98732487aae5";
        ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
        	.withBucketName(bucketName)
        	.withPrefix("bucket1"));
        
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
        	System.out.println(" - " + objectSummary.getKey() + "  " +
                    "(size = " + objectSummary.getSize() + ")");
        }
        
        System.out.println("Done..");
        
    }
    
    public void testListingS3() throws IOException{
    	
    	/*AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("srinivask_profile").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/home/srini/.aws/credentials), and is in valid format.",
                    e);
        }*/
    	
		Properties properties = new Properties();
		//String awsKeyFile = System.getenv("AWS_CREDENTIAL_FILE");
        //properties.load(new FileInputStream(awsKeyFile));
		properties.load(new FileInputStream("keys/myCredentialFile"));
        		
        AWSCredentials creds = new BasicAWSCredentials(properties.getProperty("AWSAccessKeyId"),
				properties.getProperty("AWSSecretKey"));

        AmazonS3 s3 = new AmazonS3Client(creds);
        Region usWest1 = Region.getRegion(Regions.US_WEST_1);
        s3.setRegion(usWest1);
        
        //String bucketName = "stestbuc1";
        
        String bucketName = "filterbucket" + UUID.randomUUID();
        //String key = "MyFilterKey";
        
        System.out.println("Creating bucket " + bucketName + "\n");
        s3.createBucket(bucketName);
        
        
        System.out.println("Uploading a new object to S3 from a file\n");
        s3.putObject(new PutObjectRequest(bucketName, "job1" + UUID.randomUUID(), createSampleFile1("job1" + UUID.randomUUID())));
        s3.putObject(new PutObjectRequest(bucketName, "job1" + UUID.randomUUID() , createSampleFile1("job1" + UUID.randomUUID())));
        s3.putObject(new PutObjectRequest(bucketName, "job1" + UUID.randomUUID() , createSampleFile1("job1" + UUID.randomUUID())));
        s3.putObject(new PutObjectRequest(bucketName, "job1" + UUID.randomUUID() , createSampleFile1("job1" + UUID.randomUUID())));
        s3.putObject(new PutObjectRequest(bucketName, "job1" + UUID.randomUUID() , createSampleFile1("job1" + UUID.randomUUID())));
        s3.putObject(new PutObjectRequest(bucketName, "job2" + UUID.randomUUID(), createSampleFile1("job2" + UUID.randomUUID())));
        s3.putObject(new PutObjectRequest(bucketName, "job2" + UUID.randomUUID(), createSampleFile1("job2" + UUID.randomUUID())));
        s3.putObject(new PutObjectRequest(bucketName, "job2" + UUID.randomUUID(), createSampleFile1("job2" + UUID.randomUUID())));
        
        System.out.println("Listing objects");
        ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                .withBucketName(bucketName)
                .withPrefix("job1"));
        
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            System.out.println(" - " + objectSummary.getKey() + "  " +
                               "(size = " + objectSummary.getSize() + ")");
        }
        
        
        //System.out.println("Deleting an object\n");
        //s3.deleteObject(bucketName, key);
        System.out.println();
        
    	
    }
    
	
	public void testS3() throws IOException{
		
		/*
         * The ProfileCredentialsProvider will return your [srinivask_profile]
         * credential profile by reading from the credentials file located at
         * (/home/srini/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("srinivask_profile").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/home/srini/.aws/credentials), and is in valid format.",
                    e);
        }

        AmazonS3 s3 = new AmazonS3Client(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        s3.setRegion(usWest2);

        String bucketName = "my-first-s3-bucket-" + UUID.randomUUID();
        String key = "MyObjectKey";

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon S3");
        System.out.println("===========================================\n");

        try {
            /*
             * Create a new S3 bucket - Amazon S3 bucket names are globally unique,
             * so once a bucket name has been taken by any user, you can't create
             * another bucket with that same name.
             *
             * You can optionally specify a location for your bucket if you want to
             * keep your data closer to your applications or users.
             */
            System.out.println("Creating bucket " + bucketName + "\n");
            s3.createBucket(bucketName);

            /*
             * List the buckets in your account
             */
            System.out.println("Listing buckets");
            for (Bucket bucket : s3.listBuckets()) {
                System.out.println(" - " + bucket.getName());
            }
            System.out.println();

            /*
             * Upload an object to your bucket - You can easily upload a file to
             * S3, or upload directly an InputStream if you know the length of
             * the data in the stream. You can also specify your own metadata
             * when uploading to S3, which allows you set a variety of options
             * like content-type and content-encoding, plus additional metadata
             * specific to your applications.
             */
            System.out.println("Uploading a new object to S3 from a file\n");
            s3.putObject(new PutObjectRequest(bucketName, key, createSampleFile()));

            /*
             * Download an object - When you download an object, you get all of
             * the object's metadata and a stream from which to read the contents.
             * It's important to read the contents of the stream as quickly as
             * possibly since the data is streamed directly from Amazon S3 and your
             * network connection will remain open until you read all the data or
             * close the input stream.
             *
             * GetObjectRequest also supports several other options, including
             * conditional downloading of objects based on modification times,
             * ETags, and selectively downloading a range of an object.
             */
            System.out.println("Downloading an object");
            S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
            System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
            displayTextInputStream(object.getObjectContent());

            /*
             * List objects in your bucket by prefix - There are many options for
             * listing the objects in your bucket.  Keep in mind that buckets with
             * many objects might truncate their results when listing their objects,
             * so be sure to check if the returned object listing is truncated, and
             * use the AmazonS3.listNextBatchOfObjects(...) operation to retrieve
             * additional results.
             */
            System.out.println("Listing objects");
            ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                    .withBucketName(bucketName)
                    .withPrefix("My"));
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                System.out.println(" - " + objectSummary.getKey() + "  " +
                                   "(size = " + objectSummary.getSize() + ")");
            }
            System.out.println();

            /*
             * Delete an object - Unless versioning has been turned on for your bucket,
             * there is no way to undelete an object, so use caution when deleting objects.
             */
            System.out.println("Deleting an object\n");
            s3.deleteObject(bucketName, key);

            /*
             * Delete a bucket - A bucket must be completely empty before it can be
             * deleted, so remember to delete any objects from your buckets before
             * you try to delete them.
             */
            System.out.println("Deleting bucket " + bucketName + "\n");
            s3.deleteBucket(bucketName);
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }		
		
	}
	
	
	
	
}

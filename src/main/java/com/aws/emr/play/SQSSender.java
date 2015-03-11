package com.aws.emr.play;

import java.io.FileInputStream;
import java.util.Properties;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

public class SQSSender extends AbstractExecutionThreadService{

	String m_queueName = "";
	String m_queueURL = "";
	String m_credPath = "";
	String m_bucketName = "";
	
	AmazonSQS m_sqs;
	AmazonS3 m_s3;
	
	public SQSSender(String awsCredPath, String queueName){
		m_credPath = awsCredPath;
		m_queueName = queueName;
	}
	
	public void setBucketName(String bucketName){
		m_bucketName = bucketName;
	}
	
	protected void startUp() throws Exception{
		System.out.println("Startup method...");
		
		Properties properties = new Properties();
		//String awsKeyFile = System.getenv("AWS_CREDENTIAL_FILE");
        //properties.load(new FileInputStream(awsKeyFile));
		properties.load(new FileInputStream(m_credPath));
        		
        AWSCredentials creds = new BasicAWSCredentials(properties.getProperty("AWSAccessKeyId"),
				properties.getProperty("AWSSecretKey"));
        
        // create SQS client
        m_sqs = new AmazonSQSClient(creds);
        m_sqs.setRegion(Region.getRegion(Regions.US_WEST_1));
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(m_queueName);
        m_queueURL = m_sqs.getQueueUrl(getQueueUrlRequest).getQueueUrl();
        
        // read a directory for a list of files and send it to queue
        m_s3 = new AmazonS3Client(creds);
        Region usWest1 = Region.getRegion(Regions.US_WEST_1);
        m_s3.setRegion(usWest1);
        
        if(m_bucketName.isEmpty()){
        	m_bucketName = "filterbucket7b7532a5-2afb-4762-aa80-98732487aae5";
        }
	}
	
	
	public void sendMessageToQueue(String message){
        SendMessageResult messageResult =  m_sqs.sendMessage(new SendMessageRequest(m_queueURL, message));
        System.out.println(messageResult.toString());
    }
	
	@Override
	protected void run() throws Exception {
		
		// TODO: get the listings in batch...
		System.out.println("Fetching files from s3..");
        ObjectListing objectListing = m_s3.listObjects(new ListObjectsRequest()
    		.withBucketName(m_bucketName)
    		.withPrefix("job"));
        
    
	    for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
	    	sendMessageToQueue(objectSummary.getKey());
	    }
		
	    System.out.println("Done with sending messages..");
	    // TODO: keep this service running.. 
		/*while(isRunning()){
			
		}*/
	}

	protected void shutDown() throws Exception{
		System.out.println("Sender Shutdown method...");
		//stop();
	}
}

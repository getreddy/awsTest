package com.aws.emr.play;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

public class SQSReaderTh extends AbstractExecutionThreadService{

	
	String m_queueName = "";
	String m_queueURL = "";
	
	AmazonSQS m_sqs;
	
	public SQSReaderTh(String queueName){
		m_queueName = queueName;
	}

	
	protected void startUp() throws Exception{
		
		System.out.println("Startup method...");
		
		Properties properties = new Properties();
		String awsKeyFile = System.getenv("AWS_CREDENTIAL_FILE");
        properties.load(new FileInputStream(awsKeyFile));
        		
        AWSCredentials creds = new BasicAWSCredentials(properties.getProperty("AWSAccessKeyId"),
				properties.getProperty("AWSSecretKey"));
        
        // create SQS client
        m_sqs = new AmazonSQSClient(creds);
        m_sqs.setRegion(Region.getRegion(Regions.US_WEST_1));
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(m_queueName);
        m_queueURL = m_sqs.getQueueUrl(getQueueUrlRequest).getQueueUrl();
		
	}
	
	@Override
	protected void run() throws Exception {
		
		System.out.println("SQSReader working..");
		File file = new File("LogFile.txt");
		if (file.createNewFile()){
	        System.out.println("File is created!");
	    }else{
	        System.out.println("File already exists.");
	    }
		
		while(isRunning()){
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(m_queueURL);
	        List<Message> messages = m_sqs.receiveMessage(receiveMessageRequest).getMessages();
	        if(messages != null &&  messages.size() <= 0){
	        	System.out.println("Queue is empty...wait and check again..");
	        	Thread.sleep(5000);
	        	continue;
	        }
	        
	        System.out.println("... *** Size of messages list: " +messages.size());
	        
	        for (Message message : messages) {
	            System.out.println("  Message");
	            System.out.println("    MessageId:     " + message.getMessageId());
	            System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
	            System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
	            System.out.println("    Body:          " + message.getBody());
	            for (Entry<String, String> entry : message.getAttributes().entrySet()) {
	                System.out.println("  Attribute");
	                System.out.println("    Name:  " + entry.getKey());
	                System.out.println("    Value: " + entry.getValue());
	            }
	        }
	        
			System.out.println("Looping...");
			Thread.sleep(5000);
		}		
		
	}

	protected void shutDown() throws Exception{
		System.out.println("Shutdown method...");
		//stop();
	}
	
	
}
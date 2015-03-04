package com.aws.emr.play;



public class SQSReader{

	public void readMessages(String awsCredPath, String queueName){
		SQSReaderTh obj = new SQSReaderTh(awsCredPath, queueName);
		obj.start();
	}
	
	/*public static void main(String args[]){
		
		String queueName = "";
    	String awsCredPath = "";
		if(args != null && args.length > 0){
			awsCredPath = args[0];
			queueName = args[1];
		}
		
		SQSReader sqsObj = new SQSReader();
		sqsObj.readMessages(awsCredPath, queueName);
		
		System.out.println("SQS Reader Done..");
	}*/
	
}

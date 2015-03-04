package com.aws.emr.play;



public class SQSReader{

	public void readMessages(String queueName){
		SQSReaderTh obj = new SQSReaderTh(queueName);
		obj.start();
	}
	
	/*public static void main(String args[]){
		
		String queueName = "";
		if(args != null && args.length > 0){
			queueName = args[0];
		}
		
		SQSReader sqsObj = new SQSReader();
		sqsObj.readMessages(queueName);
		
		System.out.println("SQS Reader Done..");
	}*/
	
}

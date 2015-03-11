package com.aws.emr.play;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.AttachInstancesRequest;
import com.amazonaws.services.autoscaling.model.CreateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.CreateLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.DescribeLaunchConfigurationsResult;
import com.amazonaws.services.autoscaling.model.InstanceMonitoring;
import com.amazonaws.services.autoscaling.model.LaunchConfiguration;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.util.Base64;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

public class AutoScaleCW{

	protected Region region;
	String m_queueName = "";
	String m_queueURL = "";
	String m_credPath = "";
	String m_imageName = "";
	
	AmazonSQS m_sqs;
	AWSCredentials creds;
	
	public AutoScaleCW(String awsCredPath, String queueName, String imageName){
		m_credPath = awsCredPath;
		m_queueName = queueName;
		m_imageName = imageName;
	}
	
	
	protected void startUp() throws Exception{
		System.out.println("Startup method...");
		
		Properties properties = new Properties();
		//String awsKeyFile = System.getenv("AWS_CREDENTIAL_FILE");
        //properties.load(new FileInputStream(awsKeyFile));
		properties.load(new FileInputStream(m_credPath));
        		
        creds = new BasicAWSCredentials(properties.getProperty("AWSAccessKeyId"),
				properties.getProperty("AWSSecretKey"));
        
        // create SQS client
        m_sqs = new AmazonSQSClient(creds);
        m_sqs.setRegion(Region.getRegion(Regions.US_WEST_1));
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(m_queueName);
        m_queueURL = m_sqs.getQueueUrl(getQueueUrlRequest).getQueueUrl();
	}
	
	public String attachNewInstance() throws InterruptedException{
		
		AmazonEC2Client ec2 = new AmazonEC2Client( creds );
        ec2.setRegion(region);
        
        IpPermission ipPermission = 
        		new IpPermission();
        
        /*List<String> ipRanges = new ArrayList<String>();
        ipRanges.add("0.0.0.0/0"); 
        ipPermission.withIpRanges(ipRanges)
        	            .withIpProtocol("tcp")
        	            .withFromPort(22)
        	            .withToPort(22);
        AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest =
        		new AuthorizeSecurityGroupIngressRequest();
        	    	
        authorizeSecurityGroupIngressRequest.withGroupName("launch-wizard-1")
        	                                    .withIpPermissions(ipPermission);
        ec2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);*/
        
        //String imageId = "ami-ab844dc2"
        int minInstanceCount = 1; // create 1 instance
        int maxInstanceCount = 1;
        RunInstancesRequest rir = new RunInstancesRequest(m_imageName, minInstanceCount, maxInstanceCount);
        rir.setInstanceType("t2.micro");
        rir.setKeyName("srini_ubuntu1");// give the instance the key we just created
        List<String> groups = new ArrayList<String>();
        groups.add("launch-wizard-1");
        rir.setSecurityGroups(groups);// set the instance in the group we just created
      
        RunInstancesResult result = ec2.runInstances(rir);
        Reservation reserveResult = result.getReservation();
        List<Instance> instances = reserveResult.getInstances();
        
        
        System.out.println("Launched attached instances..: " +instances.size());
        String instanceID = "";
        if(instances.size() > 0){
        	Instance id = instances.get(0);
        	instanceID = id.getInstanceId();
        	System.out.println("Launched instance ID: " +instanceID);
        } 
        
        
        
        // The wait for this instance id to be in the running state
        // instance state must be in running state before adding to Autoscale
        boolean isWaiting = true;
        while (isWaiting) {
        	
        	System.out.println("Waiting for instance to be in running state...");
	        Thread.sleep(2000);
	        
        	DescribeInstancesResult dir = ec2.describeInstances(new DescribeInstancesRequest().withFilters(
            		new Filter().withName( "image-id" ).withValues( m_imageName )
                ) );        	
        	Iterator<Reservation> ir= dir.getReservations().iterator();
        	while(ir.hasNext()){
	          Reservation rr = ir.next();
	          List<Instance> instances1 = rr.getInstances(); 
	          for(Instance ii : instances1){
	          System.out.println("ImageID: " +ii.getImageId() + " instanceID: " + ii.getInstanceId()+ " Instance State: " + ii.getState().getName());
	          if (ii.getState().getName().equals("running") ) {
	           System.out.println("Instance is in running state..");
	           isWaiting=false;
	          }
	         }
        	}
         }
        
        
        return instanceID;
	}
	
	public void testProvisioning() throws Exception{

		startUp();
		
		AmazonAutoScalingClient amazonAutoScalingClient = new AmazonAutoScalingClient(creds);
		region = Region.getRegion(Regions.US_WEST_1);
        amazonAutoScalingClient.setRegion(region);
        
        
        AmazonEC2Client ec2 = new AmazonEC2Client( creds );
        ec2.setRegion(region);
        
        /*final DescribeImagesResult imagesResult = ec2.describeImages( new DescribeImagesRequest().withFilters(
                new Filter().withName( "image-type" ).withValues( "machine" )
            ) );
        */
        
        /*DescribeInstancesResult dir = ec2.describeInstances(new DescribeInstancesRequest().withFilters(
        		new Filter().withName( "instance-id" ).withValues( "i-42652681" )
            ) );
        System.out.println("Number of Images: " +dir.getReservations().size());*/
        
        
		DescribeLaunchConfigurationsResult desRet = amazonAutoScalingClient.describeLaunchConfigurations();
		List<LaunchConfiguration> lsConfig = desRet.getLaunchConfigurations();
		
		System.out.println("Number of configs: " +lsConfig.size());
		
		
		final DescribeAvailabilityZonesResult azResult = ec2.describeAvailabilityZones();
		if(azResult.getAvailabilityZones().size() <= 0){
			System.out.println("Availability zone not found");
			return ;
		}
		System.out.println("Availability zones available: " +azResult.getAvailabilityZones().size());
		List<AvailabilityZone> zoneList = azResult.getAvailabilityZones();
		List<String> zoneArr = new ArrayList<String>();
		for(AvailabilityZone zone : zoneList){
			zoneArr.add(zone.getZoneName());
			System.out.println("ZoneName : " +zone.getZoneName());
		}
		final String availabilityZone = azResult.getAvailabilityZones().get( 0 ).getZoneName();
		
		// create config
		String configName = "SriniAmiConfig3";
		
		StringBuffer sb = new StringBuffer();
		sb.append("#!/bin/bash");
		sb.append("\n");
		sb.append("cd /home/ubuntu/awsT/awsTest");
		sb.append("\n");
		String part1 = "java -jar target/cw-aws-1.0-SNAPSHOT.jar sr keys/myCredentialFile Sriniqueue1";
		String part2 = part1 + " " + m_imageName + " 2>&1 | tee Logreader.txt &";
		sb.append(part2);
		sb.append("\n");
		sb.append("touch afterFile.txt");
		sb.append("\n");
		
		//System.out.println("Command: " +sb.toString());
		
		
		byte[] encoded = Base64.encode(sb.toString().getBytes()); 
		
		amazonAutoScalingClient.createLaunchConfiguration( new CreateLaunchConfigurationRequest()
	        .withLaunchConfigurationName( configName )
	        .withImageId( m_imageName )
	        .withInstanceType( "t2.micro")
	        .withUserData(new String(encoded))
	        .withKeyName("srini_ubuntu1")
	        .withInstanceMonitoring( new InstanceMonitoring().withEnabled( true ) ) );
		
		
		// create autoscale group..
		// Create scaling group
		String groupName = "SriniAutoScale1";
		amazonAutoScalingClient.createAutoScalingGroup( new CreateAutoScalingGroupRequest()
	      .withAutoScalingGroupName( groupName )
	      .withLaunchConfigurationName( configName )
	      .withMinSize( 1 )
	      .withMaxSize( 1 )
	      .withHealthCheckGracePeriod( 300 )
	      .withAvailabilityZones( zoneArr )
	    );
		
		String instanceID = attachNewInstance();
		System.out.println("Attaching the new running instance..:" +instanceID);
		AttachInstancesRequest attReq = new AttachInstancesRequest();
		List<String> instanceIDs  = new ArrayList<String>();
		instanceIDs.add(instanceID);
		attReq.withAutoScalingGroupName(groupName);
		attReq.setInstanceIds(instanceIDs);
		
		amazonAutoScalingClient.attachInstances(attReq);
		
		System.out.println("Done...");
	}
	
	
}

package com.aws.emr.play;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.CreateLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.DescribeLaunchConfigurationsResult;
import com.amazonaws.services.autoscaling.model.InstanceMonitoring;
import com.amazonaws.services.autoscaling.model.LaunchConfiguration;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;

public class AutoScaleTest {

	protected Region region;
	public void testAutoScale() throws FileNotFoundException, IOException{
		
		String instanceId = "Your-InstanceId";
		String accessKey = "";
		String secretKey = "";
		Properties properties = new Properties();
		String awsKeyFile = System.getenv("AWS_CREDENTIAL_FILE");
        properties.load(new FileInputStream(awsKeyFile));
        		
        AWSCredentials creds = new BasicAWSCredentials(properties.getProperty("AWSAccessKeyId"),
				properties.getProperty("AWSSecretKey"));
		AmazonAutoScalingClient amazonAutoScalingClient = new AmazonAutoScalingClient(creds);
		region = Region.getRegion(Regions.US_WEST_1);
        amazonAutoScalingClient.setRegion(region);
        
        
        AmazonEC2Client ec2 = new AmazonEC2Client( creds );
        ec2.setRegion(region);
        
        /*final DescribeImagesResult imagesResult = ec2.describeImages( new DescribeImagesRequest().withFilters(
                new Filter().withName( "image-type" ).withValues( "machine" )
            ) );
        */
        
        DescribeInstancesResult dir = ec2.describeInstances(new DescribeInstancesRequest().withFilters(
        		new Filter().withName( "instance-id" ).withValues( "i-42652681" )
            ) );
        System.out.println("Number of Images: " +dir.getReservations().size());
        
        
		DescribeLaunchConfigurationsResult desRet = amazonAutoScalingClient.describeLaunchConfigurations();
		List<LaunchConfiguration> lsConfig = desRet.getLaunchConfigurations();
		
		System.out.println("Number of configs: " +lsConfig.size());
		
		amazonAutoScalingClient.createLaunchConfiguration( new CreateLaunchConfigurationRequest()
        .withLaunchConfigurationName( "SriniAmiConfig3" )
        .withImageId( "ami-af50b5eb" )
        .withInstanceType( "t2.micro")
        .withInstanceMonitoring( new InstanceMonitoring().withEnabled( true ) ) );
		
		System.out.println("Done...");
	}
	
}

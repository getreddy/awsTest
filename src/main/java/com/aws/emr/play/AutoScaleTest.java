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
import com.amazonaws.services.autoscaling.model.CreateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.CreateLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.DescribeLaunchConfigurationsResult;
import com.amazonaws.services.autoscaling.model.InstanceMonitoring;
import com.amazonaws.services.autoscaling.model.LaunchConfiguration;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
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
		
		
		final DescribeAvailabilityZonesResult azResult = ec2.describeAvailabilityZones();
		if(azResult.getAvailabilityZones().size() <= 0){
			System.out.println("Availability zone not found");
			return ;
		}
		final String availabilityZone = azResult.getAvailabilityZones().get( 0 ).getZoneName();
		
		// create config
		String configName = "SriniAmiConfig3";
		amazonAutoScalingClient.createLaunchConfiguration( new CreateLaunchConfigurationRequest()
        .withLaunchConfigurationName( configName )
        .withImageId( "ami-c530d481" )
        .withInstanceType( "t2.micro")
        .withKeyName("srini_ubuntu1")
        .withInstanceMonitoring( new InstanceMonitoring().withEnabled( true ) ) );
		
		
		// create autoscale group..
		// Create scaling group
		String groupName = "SriniAutoScale1";
		amazonAutoScalingClient.createAutoScalingGroup( new CreateAutoScalingGroupRequest()
	      .withAutoScalingGroupName( groupName )
	      .withLaunchConfigurationName( configName )
	      .withMinSize( 1 )
	      .withMaxSize( 2 )
	      .withHealthCheckGracePeriod( 300 )
	      .withAvailabilityZones( availabilityZone )
	    );
		
		
	  
	  
		System.out.println("Done...");
	}
	
}

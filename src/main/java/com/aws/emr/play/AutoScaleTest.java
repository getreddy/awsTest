package com.aws.emr.play;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.DescribeLaunchConfigurationsResult;
import com.amazonaws.services.autoscaling.model.LaunchConfiguration;

public class AutoScaleTest {

	
	public void testAutoScale() throws FileNotFoundException, IOException{
		
		String instanceId = "Your-InstanceId";
		String accessKey = "";
		String secretKey = "";
		Properties properties = new Properties();
		String awsKeyFile = System.getenv("AWS_CREDENTIAL_FILE");
        properties.load(new FileInputStream(awsKeyFile));
        		
		AmazonAutoScalingClient amazonAutoScalingClient = new AmazonAutoScalingClient(new BasicAWSCredentials(properties.getProperty("AWSAccessKeyId"),
																					properties.getProperty("AWSSecretKey")));
		
		DescribeLaunchConfigurationsResult desRet = amazonAutoScalingClient.describeLaunchConfigurations();
		List<LaunchConfiguration> lsConfig = desRet.getLaunchConfigurations();
		
		System.out.println("Number of configs: " +lsConfig.size());
		
		System.out.println("Done...");
	}
	
}

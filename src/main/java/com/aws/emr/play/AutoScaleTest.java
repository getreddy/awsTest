package com.aws.emr.play;

import java.util.List;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.DescribeLaunchConfigurationsResult;
import com.amazonaws.services.autoscaling.model.LaunchConfiguration;

public class AutoScaleTest {

	
	public void testAutoScale(){
		
		String instanceId = "Your-InstanceId";
		String accessKey = "";
		String secretKey = "";
		AmazonAutoScalingClient amazonAutoScalingClient = new AmazonAutoScalingClient(new BasicAWSCredentials(accessKey, secretKey));
		
		DescribeLaunchConfigurationsResult desRet = amazonAutoScalingClient.describeLaunchConfigurations();
		List<LaunchConfiguration> lsConfig = desRet.getLaunchConfigurations();
		
		System.out.println("Number of configs: " +lsConfig.size());
		
		System.out.println("Done...");
	}
	
}

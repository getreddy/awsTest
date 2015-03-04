package com.aws.emr.play;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;


/**
 * 
 */
public class App {//extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
    	
    	/*int res = ToolRunner.run(new Configuration(), new App(), args);
    	System.exit(res);*/
    	
    	/*S3Test obj = new S3Test();
    	obj.testListingS3();
    	System.out.println("Done...");*/
        
    	//AutoScaleTest autoObj = new AutoScaleTest();
    	//autoObj.testAutoScale();
    	
    	//SQSTest sqsObj = new SQSTest();
    	//sqsObj.testSQS();
    	
    	String queueName = "";
    	String awsCredPath = "";
    	String opt = "";
    	String imageName = "";
		if(args != null && args.length > 0){
			opt = args[0];
			awsCredPath = args[1];
			queueName = args[2];
			imageName = args[3];
			
		}
		
		if(opt.equalsIgnoreCase("s")){
			SQSReader sqsObj = new SQSReader();
			sqsObj.readMessages(awsCredPath, queueName);
		}else if(opt.equalsIgnoreCase("a")){
			AutoScaleTest autoObj = new AutoScaleTest();
	    	autoObj.testAutoScale(imageName);
		}
    	
		System.out.println("Done..");
    	
    }

	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

    
    /*public int run(String[] args) throws Exception {
																																																						
		try {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf);
            job.setJarByClass(App.class);

            // specify a mapper
            job.setMapperClass(com.aws.emr.play.WordMapper.class);

            // specify a reducer
            job.setReducerClass(com.aws.emr.play.WordReducer.class);

            // specify output types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // specify input and output DIRECTORIES
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(TextInputFormat.class);
            //job.setInputFormatClass(WholeFileInputFormat.class);
            																																																								
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setOutputFormatClass(TextOutputFormat.class);

            return(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            System.err.println("Error during mapreduce job.");
            e.printStackTrace();
            return 2;
        }
		
	}*/

    
}
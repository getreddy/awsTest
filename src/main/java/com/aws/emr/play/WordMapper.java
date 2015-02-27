package com.aws.emr.play;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private IntWritable one = new IntWritable(1);
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        System.out.println("Reddy...: " + Thread.currentThread().getId() + "***** Value: " +value);
        
        java.lang.management.RuntimeMXBean runtime = 
        	    java.lang.management.ManagementFactory.getRuntimeMXBean();
        	java.lang.reflect.Field jvm;
			try {
				jvm = runtime.getClass().getDeclaredField("jvm");
				
				jvm.setAccessible(true);
	        	sun.management.VMManagement mgmt =  
	        	    (sun.management.VMManagement) jvm.get(runtime);
	        	java.lang.reflect.Method pid_method =  
	        	    mgmt.getClass().getDeclaredMethod("getProcessId");
	        	pid_method.setAccessible(true);

	        	int pid = (Integer) pid_method.invoke(mgmt);		
	        	
	        	System.out.println("SriniProc Process ID: " +pid);
				
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchFieldException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	        
        
        /*StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one); 
        }*/
    }
 }
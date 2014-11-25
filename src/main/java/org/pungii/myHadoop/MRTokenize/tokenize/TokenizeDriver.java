package org.pungii.myHadoop.MRTokenize.tokenize;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.pungii.myHadoop.MRTokenize.tokenize.inputformat.MyInputFormat;
import org.pungii.myHadoop.mr.SystemConfig;


public class TokenizeDriver {

	public static void main(String[] args) throws Exception {
		
		// set configuration
		Configuration conf = new Configuration();
		conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 400000);    //max size of Split
		
		Job job = new Job(conf,"Tokenizer");
		job.setJarByClass(TokenizeDriver.class);

	    // specify input format
		job.setInputFormatClass(MyInputFormat.class);
		
        //  specify mapper
		job.setMapperClass(TokenizeMapper.class);
		
		// specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// specify input and output DIRECTORIES 
		String input = SystemConfig.HDFS_URL+"digital";
        String output = SystemConfig.HDFS_URL+"output";
        
		Path inPath = new Path(input);
		Path outPath = new Path(output);
		try {                                            //  input path
			FileSystem fs = inPath.getFileSystem(conf);
			FileStatus[] stats = fs.listStatus(inPath);
			for(int i=0; i<stats.length; i++)
				FileInputFormat.addInputPath(job, stats[i].getPath());
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}			
        FileOutputFormat.setOutputPath(job,outPath);     //  output path

		// delete output directory
		try{
			FileSystem hdfs = outPath.getFileSystem(conf);
			if(hdfs.exists(outPath))
				hdfs.delete(outPath);
			hdfs.close();
		} catch (Exception e){
			e.printStackTrace();
			return ;
		}
		
		//  run the job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}

package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TaxiDataDriver extends Configured implements Tool {

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		//TODO: Handle data cleaning here using NYCTaxiEntry class
		// Also need to setup parallel, but handle later
		
		//NOTE: Data cleaning is slightly different since gps location errors counts as empty strings -> this error counting needs to be handled in the mapper

		int res = ToolRunner.run(new Configuration(), new TaxiDataDriver(), args);
		System.exit(res);
	}

	/**
	 * 
	 */
	public int run(String args[]) {
		try {
			Configuration conf = new Configuration();

			// Job to handle task 2 - Getting the total error rates
			Job job = new Job(conf, "GPSErrorRates");
			job.setJarByClass(TaxiDataDriver.class);

			// specify a Mapper
			job.setMapperClass(GPSErrorRatesMapper.class);

			// specify a Reducer
			job.setReducerClass(EfficientDriversReducer.class);

			// specify output types
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			//TODO: Depending how we handle input, we may need to change this
			// specify input and output directories
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setOutputFormatClass(TextOutputFormat.class);


			// Job to handle Task 3 - Getting the top 10 most efficient drivers
			Job job2 = new Job(conf, "Top10MostEfficientDrivers");

			job.setJarByClass(TaxiDataDriver.class);

			// specify a Mapper
			job.setMapperClass(GPSErrorRatesMapper.class);

			// specify a Reducer
			job.setReducerClass(EfficientDriversReducer.class);

			// specify output types
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			// specify input and output directories
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setOutputFormatClass(TextOutputFormat.class);

			return (job.waitForCompletion(true) && job2.waitForCompletion(true) ? 0 : 1);

		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
	}
}

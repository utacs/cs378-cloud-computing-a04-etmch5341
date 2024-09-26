package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TaxiDataDriver extends Configured implements Tool {

	/**
	 * A class to parse the arguments of the main method
	 */
	private static class Args {
		public String data;
		public String cleansed;
		public String intermediate;
		public String task1;
		public String task2;
		public String task3;

		private Args() {
		}

		public static Args parseArgs(String[] args) {
			Args parsedArgs = new Args();

			// Check if the number of arguments is correct
			if (args.length != 6) {
				System.err.println(
						"Usage: hadoop jar <jar file> <input file> <cleansed file> <intermediate file> <task1 output file> <task2 output file> <task3 output file>");
				System.exit(1);
			}

			// data is a string
			parsedArgs.data = args[0];

			// cleansed is a string
			parsedArgs.cleansed = args[1];

			// intermediate is a string
			parsedArgs.intermediate = args[2];

			// task1 is a string
			parsedArgs.task1 = args[3];

			// task2 is a string
			parsedArgs.task2 = args[4];

			// task3 is a string
			parsedArgs.task3 = args[5];

			return parsedArgs;
		}
	}

	public static <T> List<T> getListFromIterator(Iterator<T> iterator) {
		Iterable<T> iterable = () -> iterator;

		return StreamSupport
				.stream(iterable.spliterator(), false)
				.collect(Collectors.toList());
	}

	/**
	 * the main entry point into the program
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] argsStringArr) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TaxiDataDriver(), argsStringArr);
		System.exit(res);
	}

	/**
	 * A wrapper class to handle the job creation and execution
	 */
	private static class JobWrapper {
		public Job j;

		public JobWrapper(String jobName, Configuration conf,
				Class<? extends Mapper> map, Class<?> mapOutKey, Class<?> mapOutValue,
				Class<? extends Reducer> red, Class<?> redOutKey, Class<?> redOutValue,
				Class<? extends InputFormat> inputFormat, Class<? extends OutputFormat> outputFormat,
				String inputPath, String outputPath) throws IOException {
			j = new Job(conf, jobName);
			j.setJarByClass(TaxiDataDriver.class);

			j.setMapperClass(map);
			j.setMapOutputKeyClass(mapOutKey);
			j.setMapOutputValueClass(mapOutValue);

			j.setReducerClass(red);
			j.setOutputKeyClass(redOutKey);
			j.setOutputValueClass(redOutValue);

			FileInputFormat.addInputPath(j, new Path(inputPath));
			j.setInputFormatClass(inputFormat);

			FileOutputFormat.setOutputPath(j, new Path(outputPath));
			j.setOutputFormatClass(outputFormat);
		}

		public int execute() {
			try {
				if (!j.waitForCompletion(true)) {
					return 1;
				}
			} catch (InterruptedException | ClassNotFoundException | IOException e) {
				System.err.println("Error during mapreduce job.");
				e.printStackTrace();
				return 2;
			}
			return 0;
		}
	}

	/**
	 * main entry into the hadoop master program
	 * 
	 * @param argsStringArr the arguments to the program
	 */
	public int run(String[] argsStringArr) {
		// parse the arguments
		Args args = Args.parseArgs(argsStringArr);

		// create a configuration object
		Configuration conf = new Configuration();

		try {
			// ================= data cleaning ====================
			JobWrapper dataCleaner = new JobWrapper(
					"Data Cleaning",
					conf,
					DataCleanerMapper.class,
					Text.class,
					Text.class,
					DataCleanerReducer.class,
					NullWritable.class,
					Text.class,
					TextInputFormat.class,
					TextOutputFormat.class,
					args.data,
					args.cleansed);
			dataCleaner.execute();

			// ================== task 1 =========================
			JobWrapper task1 = new JobWrapper(
					"Task 1",
					conf,
					GPSErrorCountMapper.class,
					IntWritable.class,
					IntWritable.class,
					GPSErrorCountReducer.class,
					IntWritable.class,
					IntWritable.class,
					TextInputFormat.class,
					TextOutputFormat.class,
					args.cleansed,
					args.task1);
			task1.j.setNumReduceTasks(1);
			task1.execute();

			// ================== task 2 =========================
			JobWrapper task2p1 = new JobWrapper(
					"Task 2 - Part 1",
					conf,
					GPSErrorRatesMapper.class,
					Text.class,
					Text.class,
					GPSErrorRatesReducer.class,
					Text.class,
					Text.class,
					TextInputFormat.class,
					TextOutputFormat.class,
					args.cleansed,
					args.intermediate + "/task2p1");
			task2p1.execute();

			JobWrapper task2p2 = new JobWrapper(
					"Task 2 - Part 2",
					conf,
					TopKGPSErrorRateMapper.class,
					Text.class,
					Text.class,
					TopKGPSErrorRateReducer.class,
					Text.class,
					Text.class,
					KeyValueTextInputFormat.class,
					TextOutputFormat.class,
					args.intermediate + "/task2p1",
					args.task2);
			task2p2.j.setNumReduceTasks(1);
			task2p2.execute();

			// ================== task 3 =========================
			JobWrapper task3p1 = new JobWrapper(
					"Task 3 - Part 1",
					conf,
					EfficientDriversMapper.class,
					Text.class,
					Text.class,
					EfficientDriversReducer.class,
					Text.class,
					Text.class,
					TextInputFormat.class,
					TextOutputFormat.class,
					args.cleansed,
					args.intermediate + "/task3p1");
			task3p1.execute();

			JobWrapper task3p2 = new JobWrapper(
					"Task 3 - Part 2",
					conf,
					TopKDriverMapper.class,
					Text.class,
					Text.class,
					TopKDriverReducer.class,
					Text.class,
					Text.class,
					KeyValueTextInputFormat.class,
					TextOutputFormat.class,
					args.intermediate + "/task3p1",
					args.task3);
			task3p2.j.setNumReduceTasks(1);
			task3p2.execute();

		} catch (IOException e) {
			System.err.println("Error in job creation.");
			e.printStackTrace();
			return 3;
		}

		// try {
		// // TODO: Figure out how to get CSVInputFormat working format
		// Configuration conf = new Configuration();

		// // --------------------- Task 1 ----------------------

		// // Job to handle task 1 - Getting Total Error Count
		// Job job1 = new Job(conf, "GPSErrorRates");
		// job1.setJarByClass(TaxiDataDriver.class);

		// // specify a Mapper
		// job1.setMapperClass(GPSErrorCountMapper.class);

		// // specify a Reducer
		// job1.setReducerClass(GPSErrorCountReducer.class);

		// // specify output types
		// job1.setOutputKeyClass(IntWritable.class);
		// job1.setOutputValueClass(IntWritable.class);

		// FileInputFormat.addInputPath(job1, new Path(args[0]));
		// job1.setInputFormatClass(TextInputFormat.class);

		// FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		// job1.setOutputFormatClass(TextOutputFormat.class);

		// // --------------------- Task 2 ----------------------

		// // Job to handle task 2 - Getting the total error rates
		// Job job2 = new Job(conf, "GPSErrorRates");
		// job2.setJarByClass(TaxiDataDriver.class);

		// // specify a Mapper
		// job2.setMapperClass(GPSErrorRatesMapper.class);

		// // specify a Reducer
		// job2.setReducerClass(GPSErrorRatesReducer.class);

		// // specify output types
		// job2.setOutputKeyClass(Text.class);
		// job2.setOutputValueClass(Text.class);

		// // TODO: Depending how we handle input, we may need to change this
		// // specify input and output directories
		// FileInputFormat.addInputPath(job2, new Path(args[0]));
		// job2.setInputFormatClass(TextInputFormat.class);

		// FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		// job2.setOutputFormatClass(TextOutputFormat.class);

		// if (!job2.waitForCompletion(true)) {
		// return 1;
		// }

		// Job job2_1 = new Job(conf, "TopKGPSErrorRates");
		// job2_1.setJarByClass(TaxiDataDriver.class);

		// // specify a Mapper
		// job2_1.setMapperClass(TopKGPSErrorRateMapper.class);

		// // specify a Reducer
		// job2_1.setReducerClass(TopKGPSErrorRateReducer.class);

		// // specify output types
		// job2_1.setOutputKeyClass(Text.class);
		// job2_1.setOutputValueClass(Text.class);

		// FileInputFormat.addInputPath(job2_1, new Path(args[1]));
		// job2_1.setInputFormatClass(TextInputFormat.class);

		// FileOutputFormat.setOutputPath(job2_1, new Path(args[2]));
		// job2_1.setOutputFormatClass(TextOutputFormat.class);

		// // --------------------- Task 3 ----------------------

		// // Job to handle Task 3 - Getting the top 10 most efficient drivers
		// Job job3 = new Job(conf, "Top10MostEfficientDrivers");

		// job3.setJarByClass(TaxiDataDriver.class);

		// // specify a Mapper
		// job3.setMapperClass(EfficientDriversMapper.class);

		// // specify a Reducer
		// job3.setReducerClass(EfficientDriversReducer.class);

		// // specify output types
		// job3.setOutputKeyClass(Text.class);
		// job3.setOutputValueClass(Text.class);

		// // specify input and output directories
		// FileInputFormat.addInputPath(job3, new Path(args[0]));
		// job3.setInputFormatClass(TextInputFormat.class);

		// FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		// job3.setOutputFormatClass(TextOutputFormat.class);

		// return (job2.waitForCompletion(true) && job2.waitForCompletion(true) &&
		// job3.waitForCompletion(true) ? 0
		// : 1);

		// } catch (InterruptedException | ClassNotFoundException | IOException e) {
		// System.err.println("Error during mapreduce job.");
		// e.printStackTrace();
		// return 2;
		// }

		// everything is good
		return 0;
	}
}

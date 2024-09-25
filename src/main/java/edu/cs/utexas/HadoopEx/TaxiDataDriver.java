package edu.cs.utexas.HadoopEx;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
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
	 * A class to parse the arguments of the main method
	 */
	private static class Args {
		private String inputFilePath;
		private String outputFilePath;

		private Args() {
		}

		public static Args parseArgs(String[] args) {
			Args parsedArgs = new Args();

			// Check if the number of arguments is correct
			if (args.length < 2) {
				System.err.println("Usage: MainClient <input file path> <output file path>");
				System.exit(1);
			}

			// inputFilePath is a string
			parsedArgs.inputFilePath = args[0];

			// outputFilePath is a string
			parsedArgs.outputFilePath = args[1];

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
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		//TODO: Handle data cleaning here using NYCTaxiEntry class
		// Also need to setup parallel, but handle later
		
		//NOTE: Data cleaning is slightly different since gps location errors counts as empty strings -> this error counting needs to be handled in the mapper
		//Actually we may be able to handle error cleaning in mapper because we can just use the input format they specified, but we would need to run this data cleaning step twice
		Args passArgs = Args.parseArgs(args);
		Reader in = new FileReader("passArgs.inputFilePath");
		FileWriter fw = new FileWriter("data/cleaned-data.csv");

		CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
			.build();

		Iterable<CSVRecord> records = csvFormat.parse(in);

		for (CSVRecord record : records) {
			//Create entry and data clean entry
			//TODO: Need to clean for other values we need to consider (GPS data)
			NYCTaxiEntry entry = NYCTaxiEntry.fromString(record.toString());

			//Add entry to new CSV if formated correctly
			if(entry != null){				
				try (final CSVPrinter printer = new CSVPrinter(fw, csvFormat)) {
					printer.printRecord(record);
				}
			}
		}

		//Change input directory to new cleaned dataset
		args[0] = "data/cleaned-data.csv";
		int res = ToolRunner.run(new Configuration(), new TaxiDataDriver(), args);
		System.exit(res);
	}

	/**
	 * 
	 */
	public int run(String args[]) {
		try {
			Configuration conf = new Configuration();

			//--------------------- Task 1 ----------------------

			// Job to handle task 1 - Getting Total Error Count
			Job job1 = new Job(conf, "GPSErrorRates");
			job1.setJarByClass(TaxiDataDriver.class);

			// specify a Mapper
			job1.setMapperClass(GPSErrorCountMapper.class);

			// specify a Reducer
			job1.setReducerClass(GPSErrorCountReducer.class);

			// specify output types
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job1, new Path(args[0]));
			job1.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job1, new Path(args[1]));
			job1.setOutputFormatClass(TextOutputFormat.class);

			//--------------------- Task 2 ----------------------

			// Job to handle task 2 - Getting the total error rates
			Job job2 = new Job(conf, "GPSErrorRates");
			job2.setJarByClass(TaxiDataDriver.class);

			// specify a Mapper
			job2.setMapperClass(GPSErrorRatesMapper.class);

			// specify a Reducer
			job2.setReducerClass(GPSErrorRatesReducer.class);

			// specify output types
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			//TODO: Depending how we handle input, we may need to change this
			// specify input and output directories
			FileInputFormat.addInputPath(job2, new Path(args[0]));
			job2.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job2, new Path(args[1]));
			job2.setOutputFormatClass(TextOutputFormat.class);

			//--------------------- Task 3 ----------------------

			// Job to handle Task 3 - Getting the top 10 most efficient drivers
			Job job3 = new Job(conf, "Top10MostEfficientDrivers");

			job3.setJarByClass(TaxiDataDriver.class);

			// specify a Mapper
			job3.setMapperClass(EfficientDriversMapper.class);

			// specify a Reducer
			job3.setReducerClass(EfficientDriversReducer.class);

			// specify output types
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);

			// specify input and output directories
			FileInputFormat.addInputPath(job3, new Path(args[0]));
			job3.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job3, new Path(args[1]));
			job3.setOutputFormatClass(TextOutputFormat.class);

			return (job2.waitForCompletion(true) && job2.waitForCompletion(true) && job3.waitForCompletion(true) ? 0 : 1);

		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
	}
}

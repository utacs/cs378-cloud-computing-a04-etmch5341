package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EfficientDriversMapper extends Mapper<Object, Text, Text, Text> {

	// Create a counter and initialize with 1
	// private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String[] entries = value.toString().split(",");

		word.set(entries[1]);

		float earnings;
		float trip_time_min;
		try {
			earnings = Float.parseFloat(entries[16]);
			trip_time_min = Float.parseFloat(entries[4]) / 60;
		} catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
			return;
		}

		Text earnings_and_minutes = new Text();

		earnings_and_minutes.set(earnings + " " + trip_time_min);

		context.write(word, earnings_and_minutes);

		
		// StringTokenizer itr = new StringTokenizer(value.toString());
		// while (itr.hasMoreTokens()) {
		// 	word.set(itr.nextToken());
		// 	context.write(word, counter);
		// }
	}
}
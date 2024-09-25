package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GPSErrorCountMapper extends Mapper<Object, Text, Text, Text> {
	// Text object for Taxi_id
	private Text taxiID = new Text();
	// Text object for output "<# of Errors> <count>" - delim: " "
	private Text output = new Text();

	private List<String> gpsErrorEntries = new ArrayList<>();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		// StringTokenizer itr = new StringTokenizer(value.toString());
		// while (itr.hasMoreTokens()) {
		// 	word.set(itr.nextToken());
		// 	context.write(word, counter);
		// }
	}

	//Need to store the GPS errors in a new file in /results
	public void Cleanup(Context context) throws IOException, InterruptedException{
		
	}
}
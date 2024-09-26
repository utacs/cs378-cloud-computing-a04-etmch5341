package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GPSErrorRatesMapper extends Mapper<Object, Text, Text, Text> {
	// Text object for Taxi_id
	private Text taxiID = new Text();
	// Text object for output "<# of Errors> <count of taxis>" - delim: " "
	private Text output = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// StringTokenizer itr = new StringTokenizer(value.toString());
		// while (itr.hasMoreTokens()) {
		// word.set(itr.nextToken());
		// context.write(word, counter);
		// }

		String[] fields = value.toString().split(",");

		// Get the taxi ID
		String taxi = fields[0];
		taxiID.set(taxi);

		// Ensure the line has enough columns
		// reset errorCount
		int error = 0;

		// check if there is an error
		try {
			/*
			 * * pickup_longtitude - index 6
			 * * pickup_latitude - index 7
			 * * dropoff_longtitutde - index 8
			 * * dropoff_latitude - index 9
			 */

			String[] gpsData = {
					fields[6],
					fields[7],
					fields[8],
					fields[9]
			};

			if (errorCheck(gpsData)) {
				error = 1;
			}
		} catch (Exception e) {
			error = 1;
		}

		output.set(error + " 1");

		context.write(taxiID, output);
	}

	/*
	 * return - True if error and false if no error
	 */
	private boolean errorCheck(String[] gpsData) {
		for (int i = 0; i < gpsData.length; i++) {
			// Empty String check
			if (gpsData[i].equals("")) {
				return true;
			}
			// Value 0 check
			if (Float.parseFloat(gpsData[i]) == 0) {
				return true;
			}
		}
		return false;
	}
}
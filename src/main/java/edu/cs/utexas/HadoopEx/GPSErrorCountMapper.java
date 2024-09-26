package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GPSErrorCountMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
	// IntWritable object for hourOfDay
	private IntWritable hourOfDay = new IntWritable();
	// IntWritable object for error count
	private IntWritable errorCount = new IntWritable();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] fields = value.toString().split(",");

		String hour = parseHourOfDay(fields[2]);
		hourOfDay.set(Integer.parseInt(hour) + 1);
		
		// reset errorCount
		errorCount.set(0);

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
				errorCount.set(1);
			}
		} catch (Exception e) {
			errorCount.set(1);
		}

		// write to context
		context.write(hourOfDay, errorCount);
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

	private String parseHourOfDay(String s) {
		// 19 total characters
		// Will just split and get first 2 characters of second item of split
		return s.split(" ")[1].substring(0, 2);
	}

}
package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class TopKDriverMapper extends Mapper<Text, Text, Text, Text> {

	private Logger logger = Logger.getLogger(TopKDriverMapper.class);

	private PriorityQueue<MostEfficientDrivers> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<>();

	}

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] outputArr = value.toString().split(" ");
		float earningsPerMin = (Float.parseFloat(outputArr[0])) / (Float.parseFloat(outputArr[1]));

		pq.add(new MostEfficientDrivers(new Text(key), new FloatWritable(earningsPerMin)));

		if (pq.size() > 5) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {

		while (pq.size() > 0) {
			MostEfficientDrivers mostEfficientDrivers = pq.poll();
			context.write(new Text(mostEfficientDrivers.getDriverId()),
					new Text(mostEfficientDrivers.getEarningsPerMin().toString()));
			logger.info("TopKMapper PQ Status: " + pq.toString());
		}
	}

}

// public class TopKDriverMapper extends Mapper<Object, Text, Text, Text>{

// public void map(Object key, Text value, Context context)
// throws IOException, InterruptedException {
// String[] values = value.toString().split(",");
// Text driverId = new Text(values[0]);
// int earnings = Integer.parseInt(values[1].split(",")[0]);
// int time = Integer.parseInt(values[1].split(",")[1]);

// Text earningsPerMin = new Text(String.valueOf((float)earnings/time));

// context.write(driverId, earningsPerMin);

// }

// }
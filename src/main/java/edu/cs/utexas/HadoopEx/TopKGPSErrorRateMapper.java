package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;


public class TopKGPSErrorRateMapper extends Mapper<Text, Text, Text, Text> {

	private Logger logger = Logger.getLogger(TopKGPSErrorRateMapper.class);


	private PriorityQueue<WordAndCount> pq;

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
		int totalFlights = Integer.parseInt(outputArr[0]); //index 0
        float totalDelayDeparture = Float.parseFloat(outputArr[1]); //index 1
		float ratio = totalDelayDeparture/totalFlights;

		pq.add(new WordAndCount(new Text(key), new FloatWritable(ratio)) );

		if (pq.size() > 10) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {


		while (pq.size() > 0) {
			WordAndCount wordAndCount = pq.poll();
			context.write(new Text(wordAndCount.getWord()), new Text(wordAndCount.getCount().toString()));
			logger.info("TopKMapper PQ Status: " + pq.toString());
		}
	}

}
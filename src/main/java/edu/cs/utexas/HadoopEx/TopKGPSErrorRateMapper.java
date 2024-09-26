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


	private PriorityQueue<TaxiAndErrorRate> pq;

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
		int totalError = Integer.parseInt(outputArr[0]); //index 0
        float totalTaxi = Float.parseFloat(outputArr[1]); //index 1
		float rate = totalError/totalTaxi;

		pq.add(new TaxiAndErrorRate(new Text(key), new FloatWritable(rate)) );

		if (pq.size() > 10) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {


		while (pq.size() > 0) {
			TaxiAndErrorRate taxiAndErrorRate = pq.poll();
			context.write(new Text(taxiAndErrorRate.getTaxiId()), new Text(taxiAndErrorRate.getErrorRate().toString()));
			logger.info("TopKMapper PQ Status: " + pq.toString());
		}
	}

}
package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class TopKDriverReducer extends Reducer<Text, Text, Text, Text> {

    private PriorityQueue<MostEfficientDrivers> pq = new PriorityQueue<MostEfficientDrivers>(10);;

    private Logger logger = Logger.getLogger(TopKDriverReducer.class);

    /**
     * Takes in the topK from each mapper and calculates the overall topK
     * 
     * @param text
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int counter = 0;

        for (Text value : values) {
            counter = counter + 1;
            logger.info("Reducer Text: counter is " + counter);
            logger.info("Reducer Text: Add this item  "
                    + new MostEfficientDrivers(new Text(key), new FloatWritable(Float.parseFloat(value.toString())))
                            .toString());

            pq.add(new MostEfficientDrivers(new Text(key), new FloatWritable(Float.parseFloat(value.toString()))));

            logger.info("Reducer Text: " + key.toString() + " , Count: " + value.toString());
            logger.info("PQ Status: " + pq.toString());
        }

        while (pq.size() > 5) {
            pq.poll();
        }

    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("TopKReducer cleanup cleanup.");
        logger.info("pq.size() is " + pq.size());

        List<MostEfficientDrivers> values = new ArrayList<MostEfficientDrivers>(10);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }

        logger.info("values.size() is " + values.size());
        logger.info(values.toString());

        Collections.reverse(values);

        for (MostEfficientDrivers value : values) {
            context.write(value.getDriverId(), new Text(value.getEarningsPerMin().toString()));
            logger.info(
                    "TopKReducer - Top-10 Words are:  " + value.getDriverId() + "  Count:" + value.getEarningsPerMin());
        }

    }

}

// public class TopKDriverReducer extends Reducer <Text, Text, Text, Text>{
// private PriorityQueue<DriverAndEffiency> pq = new
// PriorityQueue<DriverAndEffiency>(10);
// private Logger logger = Logger.getLogger(TopKDriverReducer.class);

// public void reduce(Text key, Iterable<Text> values, Context context)
// throws IOException, InterruptedException {

// // A local counter just to illustrate the number of values here!
// int counter = 0 ;

// // size of values is 1 because key only has one distinct value
// for (Text value : values) {
// counter = counter + 1;
// logger.info("Reducer Text: counter is " + counter);
// logger.info("Reducer Text: Add this item " + new TaxiAndErrorRate(key, new
// FloatWritable(Float.parseFloat(value.toString()))).toString());

// pq.add(new TaxiAndErrorRate(new Text(key), new
// FloatWritable(Float.parseFloat(value.toString()))));

// logger.info("Reducer Text: " + key.toString() + " , Count: " +
// value.toString());
// logger.info("PQ Status: " + pq.toString());
// }

// // keep the priorityQueue size <= heapSize
// while (pq.size() > 5) {
// pq.poll();
// }

// }

// }
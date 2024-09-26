package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Iterator;



public class TopKGPSErrorRateReducer extends  Reducer<Text, Text, Text, Text> {

    private PriorityQueue<TaxiAndErrorRate> pq = new PriorityQueue<TaxiAndErrorRate>(10);;


    private Logger logger = Logger.getLogger(TopKGPSErrorRateReducer.class);


//    public void setup(Context context) {
//
//        pq = new PriorityQueue<WordAndCount>(10);ß
//    }


    /**
     * Takes in the topK from each mapper and calculates the overall topK
     * @param text
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
   public void reduce(Text key, Iterable<Text> values, Context context)
           throws IOException, InterruptedException {


       // A local counter just to illustrate the number of values here!
        int counter = 0 ;


       // size of values is 1 because key only has one distinct value
       for (Text value : values) {
           counter = counter + 1;
           logger.info("Reducer Text: counter is " + counter);
           logger.info("Reducer Text: Add this item  " + new TaxiAndErrorRate(key, new FloatWritable(Float.parseFloat(value.toString()))).toString());

           pq.add(new TaxiAndErrorRate(new Text(key), new FloatWritable(Float.parseFloat(value.toString()))));

           logger.info("Reducer Text: " + key.toString() + " , Count: " + value.toString());
           logger.info("PQ Status: " + pq.toString());
       }

       // keep the priorityQueue size <= heapSize
       while (pq.size() > 5) {
           pq.poll();
       }


   }


    public void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("TopKReducer cleanup cleanup.");
        logger.info("pq.size() is " + pq.size());

        List<TaxiAndErrorRate> values = new ArrayList<TaxiAndErrorRate>(10);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }

        logger.info("values.size() is " + values.size());
        logger.info(values.toString());


        // reverse so they are ordered in descending order
        Collections.reverse(values);


        for (TaxiAndErrorRate value : values) {
            context.write(value.getTaxiId(), new Text(value.getErrorRate().toString()));
            logger.info("TopKReducer - Top-10 Words are:  " + value.getTaxiId() + "  Count:"+ value.getErrorRate());
        }


    }

}
package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EfficientDriversReducer extends  Reducer<Text, IntWritable, Text, IntWritable> {

   public void reduce(Text text, Iterable<IntWritable> values, Context context)
           throws IOException, InterruptedException {
	   
        int total_earnings;
		int total_trip_time;
       
       for (Text value : values) {
           String earnings_and_minutes = value.toString();
           String[] itr = earnings_and_minutes.split(",");
           total_earnings += Integer.parseInt(itr[0]);
           total_trip_time += Integer.parseInt(itr[1]);
       }

       Text total_earnings_and_minutes = new Text()
       total_earnings_and_minutes.set(Integer.toString(total_earnings) + "," + Integer.toString(total_trip_time);)
       
       context.write(text, total_earnings_and_minutes);
   }

    //TODO: Add logger information to print results/store results
   public void cleanup(Context context) throws IOException, InterruptedException{
		
   }
}
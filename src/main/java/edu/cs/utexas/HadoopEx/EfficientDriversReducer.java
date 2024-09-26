package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EfficientDriversReducer extends  Reducer<Text, Text, Text, Text> {

   public void reduce(Text text, Iterable<Text> values, Context context)
           throws IOException, InterruptedException {
	   
        float total_earnings = 0;
		float total_trip_time = 0;
       
       for (Text value : values) {
           String earnings_and_minutes = value.toString();
           String[] itr = earnings_and_minutes.split(" ");
           total_earnings += Float.parseFloat(itr[0]);
           total_trip_time += Float.parseFloat(itr[1]);
       }

       Text total_earnings_and_minutes = new Text();
       total_earnings_and_minutes.set(total_earnings + " " + total_trip_time);
       
       context.write(text, total_earnings_and_minutes);
   }

    //TODO: Add logger information to print results/store results
   public void cleanup(Context context) throws IOException, InterruptedException{
		
   }
}
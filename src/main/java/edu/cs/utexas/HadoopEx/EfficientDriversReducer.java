package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EfficientDriversReducer extends  Reducer<Text, IntWritable, Text, IntWritable> {

   public void reduce(Text text, Iterable<IntWritable> values, Context context)
           throws IOException, InterruptedException {
	   
       int sum = 0;
       
       for (IntWritable value : values) {
           sum += value.get();
       }
       
       context.write(text, new IntWritable(sum));
   }

    //TODO: Add logger information to print results/store results
   public void cleanup(Context context) throws IOException, InterruptedException{
		
   }
}
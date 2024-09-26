package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class GPSErrorCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable text, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int errorCount = 0;

        for (IntWritable value : values) {
            errorCount += value.get();
        }

        // TODO: Make sure it is stored in order
        // Writes hourOfDay and number of errors
        context.write(text, new IntWritable(errorCount)); // Pretty sure this just writes to results
    }
}
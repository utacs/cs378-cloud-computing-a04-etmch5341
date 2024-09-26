package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

class DataCleanerMapper extends Mapper<Object, Text, Object, Text> {
    private final Text key = new Text("");
    private final Text line = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        NYCTaxiEntry entry = NYCTaxiEntry.fromString(line);
        if (entry != null) {
            // need to somehow send the entry without key
        }
    }
}

class DataCleanerReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

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
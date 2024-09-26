package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

class DataCleanerMapper extends Mapper<Object, Text, Text, Text> {
    private final Text line = new Text();
    private final Text blank = new Text("");

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String lineStr = value.toString();
        NYCTaxiEntry entry = NYCTaxiEntry.fromString(lineStr);
        if (entry != null) {
            line.set(lineStr);
            context.write(line, blank);
        }
    }
}

class DataCleanerReducer extends Reducer<Text, Text, NullWritable, Text> {
    private final NullWritable empty = NullWritable.get();

    public void reduce(Text text, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(empty, text);
        }
    }
}
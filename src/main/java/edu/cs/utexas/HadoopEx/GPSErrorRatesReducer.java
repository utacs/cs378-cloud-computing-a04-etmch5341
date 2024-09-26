package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GPSErrorRatesReducer extends Reducer<Text, Text, Text, Text> {

    // Text object for output "<# of Errors> <count of taxis>" - delim: " "

    public void reduce(Text text, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int totalErrors = 0; // index 0
        int totalTaxis = 0; // index 1

        for (Text v : values) {
            String[] outputArr = v.toString().split(" ");
            if (outputArr.length == 2) {
                try {
                    int error = Integer.parseInt(outputArr[0]);
                    int taxi = Integer.parseInt(outputArr[1]);

                    totalErrors += error;
                    totalTaxis += taxi;
                } catch (Exception e) {
                    continue;
                }
            }
        }
        String output = totalErrors + " " + totalTaxis;
        context.write(text, new Text(output));
    }
}
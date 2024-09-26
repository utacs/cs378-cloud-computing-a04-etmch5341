package edu.cs.utexas.HadoopEx;

import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopKDriverMapper extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            Text driverId = new Text(values[0]);
            int earnings = Integer.parseInt(values[1].split(",")[0]);
            int time = Integer.parseInt(values[1].split(",")[1]);

            Text earningsPerMin = new Text(String.valueOf((float)earnings/time));
    
            context.write(driverId, earningsPerMin);

        }

    
}
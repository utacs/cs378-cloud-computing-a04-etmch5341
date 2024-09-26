package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public class DriverAndEffiency implements Comparable<DriverAndEffiency>{

    private final Text driverId;
    private final Text earningsPerMin;

    public DriverAndEffiency(Text driverId, Text earningsPerMin){
        this.driverId = driverId;
        this.earningsPerMin = earningsPerMin;
    }

    public Text getDriverId(){
        return driverId;
    }

    public Text getEarningsPerMin(){
        return earningsPerMin;
    }

     /**
     * Compares two sort data objects by their value.
     * @param other The other DriverAndEfficiency object to compare with.
     * @return 0 if equal, negative if this < other, positive if this > other.
     */
    @Override
    public int compareTo(DriverAndEffiency other) {
        float diff = Float.parseFloat(this.earningsPerMin.toString()) - Float.parseFloat(other.earningsPerMin.toString());

        if (diff > 0) {
            return 1;
        } else if (diff < 0) {
            return -1;
        }
        return 0;
    }
}

package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;


public class MostEfficientDrivers implements Comparable<MostEfficientDrivers> {

        private final Text driverId;
        private final FloatWritable earningsPerMin;

        public MostEfficientDrivers(Text driverId, FloatWritable earningsPerMin) {
            this.driverId = driverId;
            this.earningsPerMin = earningsPerMin;
        }

        public Text getDriverId() {
            return driverId;
        }

        public FloatWritable getEarningsPerMin() {
            return earningsPerMin;
        }
    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
        @Override
        public int compareTo(TaxiAndErrorRate other) {

            float diff = earningsPerMin.get() - other.earningsPerMin.get();
            if (diff > 0) {
                return 1;
            } else if (diff < 0) {
                return -1;
            }
            return 0;
        }


        public String toString(){

            return "("+driverId.toString() +" , "+ earningsPerMin.toString()+")";
        }
    }


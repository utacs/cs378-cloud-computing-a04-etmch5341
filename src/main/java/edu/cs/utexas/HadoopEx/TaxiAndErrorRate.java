package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;


public class TaxiAndErrorRate implements Comparable<TaxiAndErrorRate> {

        private final Text taxiId;
        private final FloatWritable errorRate;

        public TaxiAndErrorRate(Text taxiId, FloatWritable errorRate) {
            this.taxiId = taxiId;
            this.errorRate = errorRate;
        }

        public Text getTaxiId() {
            return taxiId;
        }

        public FloatWritable getErrorRate() {
            return errorRate;
        }
    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
        @Override
        public int compareTo(TaxiAndErrorRate other) {

            float diff = errorRate.get() - other.errorRate.get();
            if (diff > 0) {
                return 1;
            } else if (diff < 0) {
                return -1;
            }
            return 0;
        }


        public String toString(){

            return "("+taxiId.toString() +" , "+ errorRate.toString()+")";
        }
    }


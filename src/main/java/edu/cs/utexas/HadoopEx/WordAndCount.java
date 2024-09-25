package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;


public class WordAndCount implements Comparable<WordAndCount> {

        private final Text word;
        private final FloatWritable count;

        public WordAndCount(Text word, FloatWritable count) {
            this.word = word;
            this.count = count;
        }

        public Text getWord() {
            return word;
        }

        public FloatWritable getCount() {
            return count;
        }
    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
        @Override
        public int compareTo(WordAndCount other) {

            float diff = count.get() - other.count.get();
            if (diff > 0) {
                return 1;
            } else if (diff < 0) {
                return -1;
            }
            return 0;
        }


        public String toString(){

            return "("+word.toString() +" , "+ count.toString()+")";
        }
    }


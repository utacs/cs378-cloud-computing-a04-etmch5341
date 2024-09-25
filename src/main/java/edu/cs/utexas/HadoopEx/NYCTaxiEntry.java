package edu.cs.utexas.HadoopEx;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class NYCTaxiEntry {

    public String driverID;
    public String taxiID;
    public float totalAmount;
    public float totalTime;

    // prevent instantiation from outside
    private NYCTaxiEntry() {
    }

    /**
     * Parse the line and create a NYCTaxiEntry object
     * 
     * @param line
     * @return the NYCTaxiEntry object or null if the line is invalid
     */
    public static NYCTaxiEntry fromString(String line) {
        // TODO : implement this
        // parse the line, checking for validity... might help to make a checkFloat and
        // similar functions? maybe not idk

        // check for correct number of commas, and format
        // check if all money values can be converted to float
        // check if the total fare is correct sum of the other (5) money values
        int count = 0;
        StringBuilder taxi = new StringBuilder();
        StringBuilder driver = new StringBuilder();

        StringBuilder fare = new StringBuilder();
        StringBuilder surcharge = new StringBuilder();
        StringBuilder tax = new StringBuilder();
        StringBuilder tip = new StringBuilder();
        StringBuilder tolls = new StringBuilder();
        StringBuilder time = new StringBuilder(); //total trip time for the entry
        StringBuilder total = new StringBuilder();

        //Assignment 4
        StringBuilder pickup_datetime = new StringBuilder();
        StringBuilder pickup_longtitude = new StringBuilder();
        StringBuilder pickup_latitude = new StringBuilder();
        StringBuilder dropoff_longtitude = new StringBuilder();
        StringBuilder dropoff_latitude = new StringBuilder();

        for (int i = 0; i < line.length(); i++) {
            //we should convert this into a switch statement at least...
            if (line.charAt(i) == ',') {
                count++;
            } else if (count == 0) {
                taxi.append(line.charAt(i));
            } else if (count == 1) {
                driver.append(line.charAt(i));
            //Assignment 4
            } else if (count == 2){
                pickup_datetime.append(line.charAt(i));
            //----
            } else if (count == 4){
                time.append(line.charAt(i));
            //Assignment 4
            } else if (count == 6){
                pickup_longtitude.append(line.charAt(i));
            } else if (count == 7){
                pickup_latitude.append(line.charAt(i));
            } else if (count == 8){
                dropoff_longtitude.append(line.charAt(i));
            } else if (count == 9){
                dropoff_latitude.append(line.charAt(i));
            } else if (count == 11) {
            //----
                fare.append(line.charAt(i));
            } else if (count == 12) {
                surcharge.append(line.charAt(i));
            } else if (count == 13) {
                tax.append(line.charAt(i));
            } else if (count == 14) {
                tip.append(line.charAt(i));
            } else if (count == 15) {
                tolls.append(line.charAt(i));
            } else if (count == 16) {
                total.append(line.charAt(i));
            }
        }

        float f_fare;
        float f_surcharge;
        float f_tax;
        float f_tip;
        float f_tolls;
        float f_time;
        float f_total;

        float f_pickup_longtitude, f_pickup_latitude, f_dropoff_longtitude, f_dropoff_latitude;
        if (count == 16) {
            f_fare = checkFloat(fare.toString());
            f_surcharge = checkFloat(surcharge.toString());
            f_tax = checkFloat(tax.toString());
            f_tip = checkFloat(tip.toString());
            f_tolls = checkFloat(tolls.toString());
            f_time = checkFloat(time.toString());
            f_total = checkFloat(total.toString());

            f_pickup_longtitude = checkFloat(pickup_longtitude.toString());
            f_pickup_latitude = checkFloat(pickup_latitude.toString());
            f_dropoff_longtitude = checkFloat(dropoff_longtitude.toString());
            f_dropoff_latitude = checkFloat(dropoff_latitude.toString());

            if (f_fare >= 0 && f_surcharge >= 0 && f_tax >= 0 && f_tip >= 0 && f_tolls >= 0 && f_total >= 0) {
                float f_total_calculated = f_fare + f_surcharge + f_tax + f_tip + f_tolls;
                if (Math.abs(f_total_calculated - f_total) <= 0.0001) {
                    if (f_total <= 500 && f_time > 0) {
                        NYCTaxiEntry entry = new NYCTaxiEntry();
                        entry.driverID = driver.toString();
                        entry.taxiID = taxi.toString();
                        entry.totalAmount = f_total;
                        entry.totalTime = f_time;
                        return entry;
                    }
                }
            }
        }

        return null;
    }

    /**
     * Check if the money value can be converted to a float number
     * 
     * @return value as a float if the value can be converted, -1 otherwise
     */
    public static float checkFloat(String moneyVal) {
        float amount;
        try {
            if (moneyVal == null) {
                return -1;
            }
            amount = Float.parseFloat(moneyVal);
        } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
            return -1;
        }
        return amount;
    }

    static class NYCTaxiEntrySerializer {

        /**
         * Get size of the serialization
         * 
         * @param item the NYCTaxiEntry object to be serialized
         * @throws UnsupportedEncodingException
         */
        public static int getSerializedSize(NYCTaxiEntry item) throws UnsupportedEncodingException {
            // return the size of the serialized byte array
            int driverSize = 4 + item.driverID.getBytes("UTF-8").length;
            int taxiSize = 4 + item.taxiID.getBytes("UTF-8").length;
            int totalSize = 4;

            return driverSize + taxiSize + totalSize;
        }

        /**
         * Serialize the NYCTaxiEntry object to a byte array
         * 
         * @param item the NYCTaxiEntry object to be serialized
         * @throws UnsupportedEncodingException 
         */
        public static void serialize(NYCTaxiEntry item, ByteBuffer buffer) throws UnsupportedEncodingException {
            // add the serialized driver id
            byte[] driverArr = item.driverID.getBytes("UTF-8");
            buffer.putInt(driverArr.length);
            buffer.put(driverArr);

            // add the serialized taxi id
            byte[] taxiArr = item.taxiID.getBytes("UTF-8");
            buffer.putInt(taxiArr.length);
            buffer.put(taxiArr);

            // add the serialized total amount
            buffer.putFloat(item.totalAmount);
        }

        /**
         * Deserialize the byte array to a NYCTaxiEntry object
         * 
         * @param data the byte array to be deserialized
         * @return the deserialized NYCTaxiEntry object
         * @throws UnsupportedEncodingException 
         */
        public static NYCTaxiEntry deserialize(ByteBuffer buffer) throws UnsupportedEncodingException {
            // undo the serialization
            NYCTaxiEntry entry = new NYCTaxiEntry();
            
            // get the driver id
            int driverSize = buffer.getInt();
            byte[] driverArr = new byte[driverSize];
            buffer.get(driverArr);
            entry.driverID = new String(driverArr, "UTF-8");
            entry.driverID = entry.driverID.intern();

            // get the taxi id
            int taxiSize = buffer.getInt();
            byte[] taxiArr = new byte[taxiSize];
            buffer.get(taxiArr);
            entry.taxiID = new String(taxiArr, "UTF-8");
            entry.taxiID = entry.taxiID.intern();

            // get the total amount
            entry.totalAmount = buffer.getFloat();

            // get the total trip time
            entry.totalTime = buffer.getFloat();

            return entry;
        }

    }

}

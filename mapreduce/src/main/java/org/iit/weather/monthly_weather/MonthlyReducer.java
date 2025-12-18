package org.iit.weather.monthly_weather;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MonthlyReducer extends Reducer<Text, Text, Text, Text> {

    private String getOrdinal(int month) {
        if (month == 1) return "1st";
        if (month == 2) return "2nd";
        if (month == 3) return "3rd";
        return month + "th";
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double precipSum = 0;
        double tempSum = 0;
        int count = 0;

        for (Text v : values) {
            String[] p = v.toString().split(",");
            precipSum += Double.parseDouble(p[0]);
            tempSum += Double.parseDouble(p[1]);
            count += Integer.parseInt(p[2]);
        }

        double meanTemp = tempSum / count;

        // Key format: district,year,month
        String[] parts = key.toString().split(",");
        String district = parts[0];
        int month = Integer.parseInt(parts[2]);
        String ordinalMonth = getOrdinal(month);

        String sentence =
                district + " had a total precipitation of " +
                precipSum + " hours with a mean temperature of " +
                meanTemp + " for " + ordinalMonth + " month";

        context.write(new Text(district), new Text(sentence));

        // context.write(key, new Text(precipSum + "," + meanTemp));
    }
}

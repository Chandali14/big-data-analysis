package org.iit.weather.max_precipitation;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxReducer extends Reducer<Text, Text, Text, Text> {

    private String getOrdinal(int month) {
        if (month == 1) return "1st";
        if (month == 2) return "2nd";
        if (month == 3) return "3rd";
        return month + "th";
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double maxPrecip = -1;
        String maxRecord = ""; // district,year,month
        double maxTemp = 0.0;

        for (Text value : values) {

            String line = value.toString();

            // Split KEY and VALUE
            // Example: "Ampara,2014,1\t318.0,24.58"
            String[] parts = line.split("\t");
            if (parts.length != 2) continue;

            String meta = parts[0];        // district,year,month
            String[] weather = parts[1].split(",");

            double precip = Double.parseDouble(weather[0]);  // precipitation only

            if (precip > maxPrecip) {
                maxPrecip = precip;
                maxRecord = meta;
                maxTemp = Double.parseDouble(weather[1]);
            }
        }

        // No record found
        if (maxRecord.isEmpty()) return;

        // Parse best record
        String[] metaParts = maxRecord.split(",");
        String district = metaParts[0];
        int year = Integer.parseInt(metaParts[1]);
        int month = Integer.parseInt(metaParts[2]);

        String ordinal = getOrdinal(month);

        String outputSentence =
                ordinal + " month in " + year +
                        " had the highest total precipitation of " +
                        maxPrecip + " hr";

        context.write(new Text(""), new Text(outputSentence));

        // context.write(new Text("Highest Precipitation Record"), new Text(maxRecord));
    }
}

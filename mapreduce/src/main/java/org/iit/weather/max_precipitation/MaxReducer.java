package org.iit.weather.max_precipitation;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double maxPrecip = -1;
        String maxRecord = "";

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
                maxRecord = line;
            }
        }

        context.write(new Text("Highest Precipitation Record"), new Text(maxRecord));
    }
}

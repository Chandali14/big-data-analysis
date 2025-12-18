package org.iit.weather.task1_2;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double maxPrecip = -1;
        String maxRecord = "";

        for (Text v : values) {

            String line = v.toString();
            String[] parts = line.split("\t");

            if (parts.length != 2) continue;

            String districtMonth = parts[0];
            double precip = Double.parseDouble(parts[1].split(",")[0]);

            if (precip > maxPrecip) {
                maxPrecip = precip;
                maxRecord = line;
            }
        }

        context.write(new Text("Highest Precipitation"), new Text(maxRecord));
    }
}

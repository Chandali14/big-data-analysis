package org.iit.weather.job1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double totalPrecip = 0;
        double tempSum = 0;
        int count = 0;

        for (Text v : values) {
            String[] parts = v.toString().split(",");
            totalPrecip += Double.parseDouble(parts[0]);
            tempSum += Double.parseDouble(parts[1]);
            count += Integer.parseInt(parts[2]);
        }

        double meanTemp = tempSum / count;

        context.write(key, new Text(totalPrecip + "," + meanTemp));
    }
}

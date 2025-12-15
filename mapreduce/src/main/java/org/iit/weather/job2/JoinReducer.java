package org.iit.weather.job2;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String city = "";
        String weatherRecord = "";

        for (Text v : values) {
            String[] parts = v.toString().split(",");

            if (parts[0].equals("L")) {
                city = parts[1]; // district name
            } else if (parts[0].equals("W")) {
                weatherRecord = v.toString().substring(2); // remove W,
            }
        }

        if (!city.isEmpty() && !weatherRecord.isEmpty()) {
            context.write(new Text(city), new Text(weatherRecord));
        }
    }
}

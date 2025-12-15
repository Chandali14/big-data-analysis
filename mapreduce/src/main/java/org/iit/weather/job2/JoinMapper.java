package org.iit.weather.job2;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Identify file source using pattern
        if (line.split(",").length == 2) {
            // From Job1: key,value
            // key = locationId,year,month
            // value = totalPrecip,meanTemp

            String[] parts = line.split("\t");
            String[] keyParts = parts[0].split(",");

            String locationId = keyParts[0];

            context.write(new Text(locationId),
                    new Text("W," + parts[0] + "," + parts[1]));
        } else {
            // locationData.csv
            String[] fields = line.split(",");
            if (fields.length == 8) {
                String locId = fields[0];
                String city = fields[7];

                context.write(new Text(locId),
                        new Text("L," + city));
            }
        }
    }
}

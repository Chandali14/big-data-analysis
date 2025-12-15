package org.iit.weather.job1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header
        if (line.contains("date")) return;

        String[] fields = line.split(",");

        try {
            String locationId = fields[0].trim();
            String date = fields[1].trim();
            double meanTemp = Double.parseDouble(fields[5].trim());     // temperature_2m_mean
            double precipHours = Double.parseDouble(fields[13].trim()); // precipitation_hours

            // Extract month and year
            String[] dateParts = date.split("/");
            String month = dateParts[0];
            String year = dateParts[2];

            // Filter last decade (2014–2024)
            int y = Integer.parseInt(year);
            if (y < 2014) return;

            // Key = location_id,year,month
            String mapKey = locationId + "," + year + "," + month;

            // Value = precip, temp, 1
            context.write(new Text(mapKey),
                    new Text(precipHours + "," + meanTemp + ",1"));

        } catch (Exception e) {
            // Skip bad records
        }
    }
}

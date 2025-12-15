package org.iit.weather.job3;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxPrecipMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] parts = value.toString().split(",");

        // Format: city,locationId,year,month,totalPrec,meanTemp
        String year = parts[2];
        String month = parts[3];
        float totalPrecip = Float.parseFloat(parts[4]);

        String outKey = year + "-" + month;

        context.write(new Text(outKey), new FloatWritable(totalPrecip));
    }
}

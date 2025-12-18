package org.iit.weather.task1_1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MonthlyReducer extends Reducer<Text, Text, Text, Text> {

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

        context.write(key, new Text(precipSum + "," + meanTemp));
    }
}

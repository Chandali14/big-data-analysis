package org.iit.weather.job3;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxPrecipReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {

        float max = 0;

        for (FloatWritable v : values) {
            max += v.get(); // total precipitation for that month-year
        }

        context.write(key, new FloatWritable(max));
    }
}

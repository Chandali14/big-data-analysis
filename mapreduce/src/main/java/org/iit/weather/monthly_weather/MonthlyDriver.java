package org.iit.weather.monthly_weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MonthlyDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: MonthlyDriver <weatherData> <locationData> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "District Monthly Weather Aggregation");

        job.setJarByClass(MonthlyDriver.class);

        job.setMapperClass(MonthlyMapper.class);
        job.setReducerClass(MonthlyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Add locationData.csv as distributed cache
        // job.addCacheFile(new Path(args[1]).toUri());
        job.addCacheFile(new URI(args[1] + "#locationData.csv"));

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

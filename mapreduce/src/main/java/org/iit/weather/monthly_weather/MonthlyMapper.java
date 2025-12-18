package org.iit.weather.monthly_weather;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashMap;

public class MonthlyMapper extends Mapper<LongWritable, Text, Text, Text> {

    private HashMap<String, String> districtMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {

        // Read the distributed cached file by name ("locationData.csv")
        Path[] cacheFiles = context.getLocalCacheFiles();

        if (cacheFiles == null || cacheFiles.length == 0) {
            throw new IOException("Distributed cache file not found!");
        }

        for (Path p : cacheFiles) {

            // Hadoop stores the cached file in working directory with alias name
            File file = new File(p.getName());  // <-- Important change

            if (!file.exists()) {
                throw new FileNotFoundException("Cached file not found: " + file.getAbsolutePath());
            }

            BufferedReader reader = new BufferedReader(new FileReader(file));
            String row;

            while ((row = reader.readLine()) != null) {

                if (row.contains("location_id")) continue; // skip header

                String[] parts = row.split(",");
                if (parts.length >= 8) {
                    String locId = parts[0].trim();
                    String district = parts[7].trim();
                    districtMap.put(locId, district);
                }
            }
            reader.close();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header
        if (line.contains("temperature_2m_mean")) return;

        String[] f = line.split(",");
        if (f.length < 14) return;

        try {
            String locId = f[0].trim();
            String date = f[1].trim();
            double meanTemp = Double.parseDouble(f[5].trim());
            double precipHours = Double.parseDouble(f[13].trim());

            // Lookup district from the cached map
            String district = districtMap.get(locId);
            if (district == null) return;

            // Parse date
            String[] d = date.split("/");
            String month = d[0];
            String year = d[2];

            // Filter last decade
            if (Integer.parseInt(year) < 2014) return;

            // Emit key-value
            String mapKey = district + "," + year + "," + month;
            String mapVal = precipHours + "," + meanTemp + ",1";

            context.write(new Text(mapKey), new Text(mapVal));

        } catch (Exception e) {
            // Ignore bad records
        }
    }
}

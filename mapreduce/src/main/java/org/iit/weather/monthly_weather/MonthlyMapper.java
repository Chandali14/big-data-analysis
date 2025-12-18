package org.iit.weather.task1_1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashMap;

public class MonthlyMapper extends Mapper<LongWritable, Text, Text, Text> {

    private HashMap<String, String> districtMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {

        // Load locationData.csv from distributed cache
        Path[] cacheFiles = context.getLocalCacheFiles();

        if (cacheFiles != null) {
            for (Path p : cacheFiles) {

                BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
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

            // Lookup district
            String district = districtMap.get(locId);
            if (district == null) return;

            // Parse date
            String[] d = date.split("/");
            String month = d[0];
            String year = d[2];

            // Filter past decade (2014–2024)
            if (Integer.parseInt(year) < 2014) return;

            // Key: District-Year-Month
            String mapKey = district + "," + year + "," + month;

            // Value: precipitation_hours,meanTemp,1
            String mapValue = precipHours + "," + meanTemp + ",1";

            context.write(new Text(mapKey), new Text(mapValue));

        } catch (Exception e) {
            // Skip bad rows
        }
    }
}

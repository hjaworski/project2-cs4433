import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;


import java.util.ArrayList;
import java.util.List;

public class KMeans {

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text>{
        private List<List<Double>> centroids = new ArrayList<List<Double>>();

        public void setup(Context context) throws IOException{
            Path centroidsPath = new Path(context.getConfiguration().get("seeds file"));
            List<String> centroidLines = Fileutil.readLines(new File(CentroidsPath.toString()));
            for (String line : centroidLines){
                String[] fields = line.split(",");
                List<Double> centroid = new ArrayList<Double>();
                for (int i = 0; i < fields.length; i++){
                    centroid.add(Double.parseDouble(fields[i]));
                }
                centroids.add(centroid);
            }
        }
        public void map(Object key, Text value, Context context)
    }
}
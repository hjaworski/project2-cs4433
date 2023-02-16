import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.Configuration.*;
import javax.security.auth.login.Configuration;

public class OptimizationKMeans {

    public static class Map1 extends Mapper<FloatWritable, Text, IntWritable, Text>{
        private final static IntWritable one= new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] data = value.toString().split(",");
            Double x = Double.parseDouble(data[0]);
            Double y = Double.parseDouble(data[1]);

            int nearestCentroid = findNearestCentroid(x,y);
            word.set(x + "," + y);
            context.write(new IntWritable(nearestCentroid), word);
        }

        private int findNearestCentroid(double x, double y){
// algerbraic calculations
            return 1;
        }
    }
    public static class Combine extends Reducer<IntWritable, Text, IntWritable, Text>{
        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            StringBuilder sb = new StringBuilder();
            for(Text val : values){
                sb.append(val.toString()).append(",");
            }
            result.set(sb.toString());
            context.write(key, result);
        }
    }
    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text>{
        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            double sumX = 0, sumY = 0;
            int count = 0;
            for (Text val: values){
                String[] data = val.toString().split(",");
                Double x = Double.parseDouble(data[0]);
                Double y = Double.parseDouble(data[1]);
                sumX += x;
                sumY += y;
                count++;
            }
            double newX = sumX / count;
            double newY = sumY / count;

            result.set(newX + "," + newY);
            context.write(key,result);
        }
    }
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration;
        Job job = Job.getInstance(Configuration, "kmeans MapReduce");
        job.setJarByClass(OptimizationKMeans.class);
        job.setMapperClass(Map1.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:/Users/Hannah/Desktop/cs4433/project2-cs4433/data.csv"));
        FileInputFormat.addInputPath(job, new Path("C:/Users/Hannah/Desktop/cs4433/project2-cs4433/seeds.csv"));
        FileOutputFormat.setOutputPath(job, new Path("C:/Users/Hannah/Desktop/cs4433/project2-cs4433/outputCombiner"));

        job.getConfiguration().setInt("k", 10);

        // Submit the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }}

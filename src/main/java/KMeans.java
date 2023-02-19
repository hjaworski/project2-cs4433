import java.io.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.lang.Math;
import java.util.ArrayList;
import java.util.Random;
import org.apache.hadoop.io.Text;


import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

//public class KMeans {

//    public static class KMeansMapper extends Mapper<Object, Text, Text, Text>{
//        private List<List<Double>> centroids = new ArrayList<List<Double>>();
//
//        public void setup(Context context) throws IOException{
//            Path centroidsPath = new Path(context.getConfiguration().get("seeds file"));
//            List<String> centroidLines = Fileutil.readLines(new File(CentroidsPath.toString()));
//            for (String line : centroidLines){
//                String[] fields = line.split(",");
//                List<Double> centroid = new ArrayList<Double>();
//                for (int i = 0; i < fields.length; i++){
//                    centroid.add(Double.parseDouble(fields[i]));
//                }
//                centroids.add(centroid);
//            }
//        }
//        public void map(Object key, Text value, Context context)
//    }
//// the following goes in a test file. select with code two csvs are beign used. - hj
//    public void TaskA() throws Exception{
//        //1 second
//        String[] input = new String[2];
//        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/MyPage.csv";
//        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskA";
//
//        CanadaCanadaCanada.main(input);
//    }

//public static class Coordinate implements Writable {
//
//    public int x;
//    public int y;
//    public int weight;
//
//    public Coordinate() {
//        x = 0;
//        y = 0;
//    }
//
//    public Coordinate(int x, int y) {
//        this.x = x;
//        this.y = y;
//        weight = 1;
//    }
//
//    public Coordinate(int x, int y, int weight) {
//        this.x = x;
//        this.y = y;
//        this.weight = weight;
//    }
//
//    @Override
//    public void write(DataOutput dataOutput) throws IOException {
//        dataOutput.writeInt(x);
//        dataOutput.writeInt(y);
//        dataOutput.writeInt(weight);
//    }
//
//    @Override
//    public void readFields(DataInput dataInput) throws IOException {
//        x = dataInput.readInt();
//        y = dataInput.readInt();
//        weight = dataInput.readInt();
//    }
//
//    public double distance(Coordinate other) {
//        return Math.hypot(x - other.x, y - other.y);
//    }
//
//    public String toString(){
//        return x+","+y+","+weight;
//    }
//
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        Coordinate that = (Coordinate) o;
//        return x == that.x && y == that.y;
//    }
//
//    @Override
//    public int hashCode() {
//        return Objects.hash(x, y);
//    }
//}

}
public class KMeans {

    public static class Poin {
        float x;
        float y;

        public Point(float pt_x, float pt_y) {
            this.x = pt_x;
            this.y = pt_y;
        }

        public String toString() {
            return Float.toString(this.x) + "," + Float.toString(this.y);
        }
    }

    public static class ClosestCentroidMapper
            extends Mapper<Object, Text, Text, Text> {

        private float distance(float x1, float x2, float y1, float y2) {
            return (float) Math.sqrt((y2 - y1) * (y2 - y1) + (x2 - x1) * (x2 - x1));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // get input point
            String[] record = value.toString().split(",");
            float x_value = Float.valueOf(record[0]);
            float y_value = Float.valueOf(record[1]);
            Point pt = new Point(x_value, y_value);

            // get centroids
            String[] cens = context.getConfiguration().get("centroids").toString().split(" "); // "" "1,2" "2,3"

            // find closest centroid to input point
            Point closest_centroid = null;
            float closest_distance = Float.MAX_VALUE;
            for (String cen : cens) {
                float cent_x = Float.parseFloat(cen.split(",")[0]);
                float cent_y = Float.parseFloat(cen.split(",")[1]);
                float dist = distance(pt.x, cent_x, pt.y, cent_y);
                if (dist < closest_distance) {
                    closest_centroid = new Point(cent_x, cent_y);
                    closest_distance = dist;
                }
            }

            // output closest centroid to pt
            context.write(new Text(closest_centroid.toString()), new Text(pt.toString()));
        }
    }

    public static class CentroidRecalculatorCombiner
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            // calculate center of all points in values to get new centroid

            // sum up totals
            float x_total = 0;
            float y_total = 0;
            float count = 0;
            for (Text pt : values) {
                String[] coords = pt.toString().split(",");
                float x = Float.valueOf(coords[0]);
                float y = Float.valueOf(coords[1]);
                x_total += x;
                y_total += y;
                count++;
            }

            // write new centroid pt to output file xtotal,ytotal,count
            context.write(key,
                    new Text(String.valueOf(x_total) + "," + String.valueOf(y_total) + "," + String.valueOf(count)));
        }
    }

    public static class CentroidRecalculatorReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            // calculate center of all points in values to get new centroid

            ArrayList<String> points = new ArrayList<>();
            // sum up totals
            float x_total = 0;
            float y_total = 0;
            float count = 0;
            for (Text pt : values) {
                String[] data = pt.toString().split(",");
                points.add(data[0] + "," + data[1]);
                float x = Float.valueOf(data[0]);
                float y = Float.valueOf(data[1]);
                float c = Float.valueOf(data[2]);
                x_total += x;
                y_total += y;
                count += c;
            }

            float new_centroid_x = x_total / count;
            float new_centroid_y = y_total / count;

            String out_style = context.getConfiguration().get("outputStyle").toString();

            // write new centroid pt to output file
            context.write(new Text("Centroid"),
                    new Text(String.valueOf(new_centroid_x) + "," + String.valueOf(new_centroid_y)));
            if (out_style.equals("b")) {
                for (String pt : points) {
                    context.write(new Text("Point"), new Text(pt));
                }
            }
        }
    }

    public void debug(String[] args) throws Exception {
        // initiliaze job
        Configuration conf = new Configuration();
        conf.set("centroids", args[2]);
        conf.set("outputStyle", args[4]);
        Job job = Job.getInstance(conf, "K means");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(ClosestCentroidMapper.class);
        job.setCombinerClass(CentroidRecalculatorCombiner.class);
        job.setReducerClass(CentroidRecalculatorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // run job
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        // initiliaze job
        Configuration conf = new Configuration();
        conf.set("centroids", args[2]);
        conf.set("outputStyle", args[4]);
        Job job = Job.getInstance(conf, "K means");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(ClosestCentroidMapper.class);
        job.setCombinerClass(CentroidRecalculatorCombiner.class);
        job.setReducerClass(CentroidRecalculatorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // run job
        job.waitForCompletion(true);
    }
}
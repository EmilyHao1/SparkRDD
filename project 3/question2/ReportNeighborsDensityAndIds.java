import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class ReportNeighborsDensityAndIds {

    public static class firstMapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text(1+""), new Text("cells," + value));
        }
    }


    public static class secondMapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String id = value.toString().split(",")[0];
            context.write(new Text(1+""), new Text("Top50,"+id));
        }
    }


    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, String> indexandSum = new HashMap<String, String>();
            ArrayList<String> lst = new ArrayList<String>();
            int xCount = 0;
            int topId = 0;
            double avg = 0.00;
            double relativeDensity = 0.00;
            double[] density = new double[250000];
            String neighborsIDs = "";
            String neighborsDensity = "";

            for(Text v:values){
                String[] lines = v.toString().split(",");
                if(lines[0].equals("cells")){
                    indexandSum.put(lines[1], lines[2]); // 250000 * (Key: ID + Value: SUM)
                }
                if(lines[0].equals("Top50")){
                    lst.add(lines[1]);
                }
            }

            for(int i = 1; i <= 250000; i++){

                xCount = Integer.parseInt(indexandSum.get(String.valueOf(i)));

                if(i == 1){
                    avg = (Integer.parseInt(indexandSum.get(String.valueOf(i+1))) + Integer.parseInt(indexandSum.get(String.valueOf(i+500))) + Integer.parseInt(indexandSum.get(String.valueOf(i+501))))/ 3.0;
                }

                else if(i == 500){
                    avg = (Integer.parseInt(indexandSum.get(String.valueOf(i-1))) + Integer.parseInt(indexandSum.get(String.valueOf(i+499))) + Integer.parseInt(indexandSum.get(String.valueOf(i+500))))/ 3.0;
                }

                else if(i == 250000-499){
                    avg = (Integer.parseInt(indexandSum.get(String.valueOf(i-500))) + Integer.parseInt(indexandSum.get(String.valueOf(i-499))) + Integer.parseInt(indexandSum.get(String.valueOf(i+1))))/ 3.0;
                }

                else if(i == 250000){
                    avg = (Integer.parseInt(indexandSum.get(String.valueOf(i-1))) + Integer.parseInt(indexandSum.get(String.valueOf(i-500))) + Integer.parseInt(indexandSum.get(String.valueOf(i-501))))/ 3.0;
                }

                else if(i>=2 && i<= 499){
                    avg = (Integer.parseInt(indexandSum.get(String.valueOf(i-1))) + Integer.parseInt(indexandSum.get(String.valueOf(i+1))) + Integer.parseInt(indexandSum.get(String.valueOf(i+500))) + Integer.parseInt(indexandSum.get(String.valueOf(i+499))) + Integer.parseInt(indexandSum.get(String.valueOf(i+501)))) / 5.0;
                }

                else if(i >= 250000-498 && i<=250000-1){
                    avg = (Integer.parseInt(indexandSum.get(String.valueOf(i-1))) + Integer.parseInt(indexandSum.get(String.valueOf(i+1))) + Integer.parseInt(indexandSum.get(String.valueOf(i-500))) + Integer.parseInt(indexandSum.get(String.valueOf(i-501))) + Integer.parseInt(indexandSum.get(String.valueOf(i-499)))) / 5.0;
                }

                else if(i % 500 == 1){
                    avg = (Integer.parseInt(indexandSum.get(String.valueOf(i-500))) + Integer.parseInt(indexandSum.get(String.valueOf(i+500))) + Integer.parseInt(indexandSum.get(String.valueOf(i+1))) + Integer.parseInt(indexandSum.get(String.valueOf(i-499))) + Integer.parseInt(indexandSum.get(String.valueOf(i+501)))) / 5.0;
                }

                else if(i % 500 == 0){
                    avg = (Integer.parseInt(indexandSum.get(String.valueOf(i-500))) + Integer.parseInt(indexandSum.get(String.valueOf(i+500))) + Integer.parseInt(indexandSum.get(String.valueOf(i-1))) + Integer.parseInt(indexandSum.get(String.valueOf(i-501))) + Integer.parseInt(indexandSum.get(String.valueOf(i+499)))) / 5.0;
                }

                else{
                    avg = (Integer.parseInt(indexandSum.get(String.valueOf(i-1))) + Integer.parseInt(indexandSum.get(String.valueOf(i+1))) + Integer.parseInt(indexandSum.get(String.valueOf(i-500))) + Integer.parseInt(indexandSum.get(String.valueOf(i+500))) + Integer.parseInt(indexandSum.get(String.valueOf(i-501))) + Integer.parseInt(indexandSum.get(String.valueOf(i-499))) + Integer.parseInt(indexandSum.get(String.valueOf(i+499))) + Integer.parseInt(indexandSum.get(String.valueOf(i+501)))) / 8.0;
                }

                relativeDensity = xCount / avg;
                density[i-1] = relativeDensity;
            }

            for(int i = 0; i < 50; i++){

                topId = Integer.parseInt(lst.get(i));

                if(topId == 1){
                    neighborsIDs = (topId+1) + "," + (topId+500) + "," + (topId+501);
                    neighborsDensity = density[topId+1-1] + "," + density[topId+500-1] + "," + density[topId+501-1];
                }

                else if(topId == 500){
                    neighborsIDs = (topId-1) + "," + (topId+499) + "," + (topId+500);
                    neighborsDensity = density[topId-1-1] + "," + density[topId+499-1] + "," + density[topId+500-1];
                }

                else if(topId == 250000-499){
                    neighborsIDs = (topId-500) + "," + (topId-499) + "," + (topId+1);
                    neighborsDensity = density[topId-500-1] + "," + density[topId-499-1] + "," + density[topId+1-1];
                }

                else if(topId == 250000){
                    neighborsIDs = (topId-1) + "," + (topId-500) + "," + (topId-501);
                    neighborsDensity = density[topId-1-1] + "," + density[topId-500-1] + "," + density[topId-501-1];
                }

                else if(topId>=2 && topId<= 499){
                    neighborsIDs = (topId-1) + "," + (topId+1) + "," + (topId+500) + "," + (topId+499) + "," + (topId+501);
                    neighborsDensity = density[topId-1-1] + "," + density[topId+1-1] + "," + density[topId+500-1] + "," + density[topId+499-1] + "," + density[topId+501-1];
                }

                else if(topId >= 250000-498 && topId<=250000-1){
                    neighborsIDs = (topId-1) + "," + (topId+1) + "," + (topId-500) + "," + (topId-501) + "," + (topId-499);
                    neighborsDensity = density[topId-1-1] + "," + density[topId+1-1] + "," + density[topId-500-1] + "," + density[topId-501-1] + "," + density[topId-499-1];
                }

                else if(topId % 500 == 1){
                    neighborsIDs = (topId-500) + "," + (topId+500) + "," + (topId+1) + "," + (topId-499) + "," + (topId+501);
                    neighborsDensity = density[topId-500-1] + "," + density[topId+500-1] + "," + density[topId+1-1] + "," + density[topId-499-1] + "," + density[topId+501-1];
                }

                else if(topId % 500 == 0){
                    neighborsIDs = (topId-500) + "," + (topId+500) + "," + (topId-1) + "," + (topId-501) + "," + (topId+499);
                    neighborsDensity = density[topId-500-1] + "," + density[topId+500-1] + "," + density[topId-1-1] + "," + density[topId-501-1] + "," + density[topId+499-1];
                }

                else{
                    neighborsIDs = (topId-1) + "," + (topId+1) + "," + (topId-500) + "," + (topId+500) + "," + (topId-501) + "," + (topId-499) + "," + (topId+499) + "," + (topId+501);
                    neighborsDensity = density[topId-1-1] + "," + density[topId+1-1] + "," + density[topId-500-1] + "," + density[topId+500-1] + "," + density[topId-501-1] + "," + density[topId-499-1] + "," + density[topId+499-1] + "," +density[topId+501-1];
                }

                context.write(new Text(neighborsIDs), new Text(neighborsDensity));
            }

        }
    }

    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ReportNeighborsDensityAndIds");
        job.setJarByClass(ReportNeighborsDensityAndIds.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job,new Path("/Users/daojun/Desktop/mapreduce/output_ReportID/part-r-00000"), TextInputFormat.class, firstMapper.class);
        MultipleInputs.addInputPath(job,new Path("/Users/daojun/Desktop/mapreduce/output_Report50ID/part-r-00000"), TextInputFormat.class, secondMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}


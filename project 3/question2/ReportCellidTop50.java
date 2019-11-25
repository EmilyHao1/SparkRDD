import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class ReportCellidTop50 {

    static class Map_First extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] xAndy = value.toString().split(",");
            int x = Integer.parseInt(xAndy[0]);
            int y = Integer.parseInt(xAndy[1]);
            int x_index = (int)Math.ceil(x / 20.0);
            int y_index = 500 - (int)Math.ceil(y / 20.0);
            int cell_index = 500 * y_index + x_index;
            context.write(new Text(cell_index + ""), new Text(x + "," + y)); //
        }
    }

    public static class Reduce_First extends Reducer<Text, Text, NullWritable, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(Text v: values){
                sum = sum + 1;
            }
            context.write(NullWritable.get(),new Text(key + "," +sum)); // output key is 1, output value is cell_index + "," + sum
        }
    }

    static class Map_Second extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text(1+""), value);
        }

    }

    public static class Reduce_Second extends Reducer<Text, Text, NullWritable, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            HashMap<String, String> indexandSum = new HashMap<String, String>();
            int xCount = 0;
            double avg = 0.00;
            double relativeDensity = 0.00;
            double[] density = new double[250000];
            int position = 0;
            int[] cellIDs = new int[50];

            for(Text v:values){
                String[] lines = v.toString().split(",");
                indexandSum.put(lines[0], lines[1]);
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

            double[] densityCopy = Arrays.copyOf(density, 250000);
            Arrays.sort(densityCopy);
            for(int i = 250000 - 1; i >= 250000 - 50; i--){
                position = getIndex(density, densityCopy[i]) + 1; // report top 50 cellID
                context.write(NullWritable.get(),new Text(position + "," + densityCopy[i]));
            }
        }
    }

    public static int getIndex(double[] arr, double a){
        for(int i = 0; i < arr.length; i++){
            if(arr[i] == a){
                return i;
            }
        }
        return -1;
    }

    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
            JobConf conf = new JobConf(ReportCellidTop50.class);

            Job job1 = new Job(conf, "job1");
            job1.setJarByClass(ReportCellidTop50.class);
            job1.setMapperClass(Map_First.class);
            job1.setReducerClass(Reduce_First.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(NullWritable.class);
            job1.setOutputValueClass(Text.class);

            ControlledJob ctrjob1 = new ControlledJob(conf);
            ctrjob1.setJob(job1);

            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[1]));

            Job job2 = new Job(conf, "job2");
            job2.setJarByClass(ReportCellidTop50.class);
            job2.setMapperClass(Map_Second.class);
            job2.setReducerClass(Reduce_Second.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(NullWritable.class);
            job2.setOutputValueClass(Text.class);

            ControlledJob ctrljob2 = new ControlledJob(conf);
            ctrljob2.setJob(job2);
            ctrljob2.addDependingJob(ctrjob1);

            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            JobControl jobCtrl = new JobControl("myCtrl");

            jobCtrl.addJob(ctrjob1);
            jobCtrl.addJob(ctrljob2);

            Thread t = new Thread(jobCtrl);
            t.start();

            while(true){
                if(jobCtrl.allFinished()){
                    System.out.println(jobCtrl.getSuccessfulJobList());
                    jobCtrl.stop();
                    break;
                }
            }

        }
}

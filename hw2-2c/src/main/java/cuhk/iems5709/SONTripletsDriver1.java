package cuhk.iems5709;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SONTripletsDriver1 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SONTripletsDriver1.class);
        job.setMapperClass(SONTripletsMapper1.class);
        job.setReducerClass(SONTripletsReducer1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//        FileInputFormat.addInputPath(job,new Path("src/dataset"));
//        FileOutputFormat.setOutputPath(job,new Path("src/output/SONTriples1output"));

        int numMappers=Integer.valueOf(args[0]);
        int numReducers=Integer.valueOf(args[1]);
        long fileSize=Integer.valueOf(args[2]);
        FileInputFormat.setMaxInputSplitSize(job,fileSize/numMappers);
        job.setNumReduceTasks(numReducers);
//        FileInputFormat.setMaxInputSplitSize(job,221811422/20);
//        job.setNumReduceTasks(2);
        FileInputFormat.addInputPath(job,new Path("/user/s1155164941/shakespeare_basket"));
        FileOutputFormat.setOutputPath(job,new Path("/user/s1155164941/output/hw2/2c-1"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

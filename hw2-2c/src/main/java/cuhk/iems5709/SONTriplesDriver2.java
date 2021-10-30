package cuhk.iems5709;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class SONTriplesDriver2 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SONTriplesDriver2.class);
        job.setMapperClass(SONTriplesMapper2.class);
        job.setReducerClass(SONTriplesReducer2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        String uri="/user/s1155164941/output/hw2/2b-1/part-r-00000#candidatePairs";
//        String uri="/user/s1155164941/output/hw2/2c-1/part-r-00000";
        String uri="/user/s1155164941/output/hw2/2c-1/merge";
//        DistributedCache.addCacheFile(new URI(uri),conf);
        int numMappers=Integer.valueOf(args[0]);
//        int numReducers=Integer.valueOf(args[1]);
        long fileSize=Integer.valueOf(args[1]);
        FileInputFormat.setMaxInputSplitSize(job,fileSize/numMappers);
//        job.setNumReduceTasks(numReducers);
        job.addCacheFile(new URI(uri));
        FileInputFormat.addInputPath(job,new Path("/user/s1155164941/shakespeare_basket"));
        FileOutputFormat.setOutputPath(job,new Path("/user/s1155164941/output/hw2/2c-2"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

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

public class SONDriver2 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://dicvmc2.ie.cuhk.edu.hk:8020");
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("yarn.resourcemanager.hostname", "dicvmc2.ie.cuhk.edu.hk");
        Job job = Job.getInstance(conf);
        job.setJarByClass(SONDriver2.class);
        job.setMapperClass(SONMapper2.class);
        job.setReducerClass(SONReducer2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        job.addCacheFile(new URI("src/output/SON1output/part-r-00000"));
//        FileInputFormat.addInputPath(job,new Path("src/dataset"));
//        FileOutputFormat.setOutputPath(job,new Path("src/output/SON2output"));
//        job.addCacheFile(new URI("/user/s1155164941/output/hw2/2b-1/part-r-00000"));

//        String uri="/user/s1155164941/output/hw2/2b-1/part-r-00000#candidatePairs";
        String uri="/user/s1155164941/output/hw2/2b-1/part-r-00000";
//        DistributedCache.addCacheFile(new URI(uri),conf);
        job.addCacheFile(new URI(uri));
        FileInputFormat.addInputPath(job,new Path("/user/s1155164941/shakespeare_basket"));
        FileOutputFormat.setOutputPath(job,new Path("/user/s1155164941/output/hw2/2b-2"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

package cuhk.iems5709;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class SONMapper2 extends Mapper<LongWritable, Text,Text, IntWritable> {
    Set<String> candidatePairs=new HashSet<>();
    int numBaskets=0;
    Map<String,Integer> pairsMap=new HashMap<>();
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String candidatePair;
        URI[] cacheFiles=context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(cacheFiles[0].toString()))));
//        BufferedReader bufferedReader = new BufferedReader(new FileReader(cacheFiles[0].getPath()));
//        BufferedReader bufferedReader = new BufferedReader(new FileReader("./candidatePairs"));
        while ((candidatePair=bufferedReader.readLine())!=null){
            candidatePairs.add(candidatePair);
        }
        System.out.println("numCandidatePairs: "+candidatePairs.size());
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        numBaskets++;
        // count pairs that only appear in candidate pairs
        String[] words=value.toString().split(" ");
        for (int i = 0; i < words.length; i++) {
            for (int j = i+1; j < words.length; j++) {
                String[] s={words[i], words[j]};
                Arrays.sort(s);
                if (candidatePairs.contains(Arrays.toString(s))){
                    Integer count = pairsMap.get(Arrays.toString(s));
                    if (count==null){
                        pairsMap.put(Arrays.toString(s),1);
                    }else {
                        pairsMap.put(Arrays.toString(s),count+1);
                    }
                }
            }
        }
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        for (Map.Entry <String,Integer> entry: pairsMap.entrySet()) {
            context.write(new Text(entry.getKey()),new IntWritable(entry.getValue()));
        }
        System.out.println("numBaskets in current mapper: "+numBaskets);
        context.write(new Text("numBaskets"),new IntWritable(numBaskets));
    }
}

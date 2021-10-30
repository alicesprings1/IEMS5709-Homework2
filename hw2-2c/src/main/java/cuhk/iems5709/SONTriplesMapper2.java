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

public class SONTriplesMapper2 extends Mapper<LongWritable, Text,Text, IntWritable> {
    Set<String> candidateTriples =new HashSet<>();
    int numBaskets=0;
    Map<String,Integer> triplesMap =new HashMap<>();
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String candidateTriple;
        URI[] cacheFiles=context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(cacheFiles[0].toString()))));
//        BufferedReader bufferedReader = new BufferedReader(new FileReader(cacheFiles[0].getPath()));
//        BufferedReader bufferedReader = new BufferedReader(new FileReader("./candidatePairs"));
        while ((candidateTriple=bufferedReader.readLine())!=null){
            candidateTriples.add(candidateTriple);
        }
        System.out.println("numCandidateTriples: "+ candidateTriples.size());
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        numBaskets++;
        // count pairs that only appear in candidate pairs
        String[] words=value.toString().split(" ");
        for (int i = 0; i < words.length; i++) {
            for (int j = i+1; j < words.length; j++) {
                for (int k = j+1; k < words.length; k++) {
                    String[] s={words[i], words[j],words[k]};
                    Arrays.sort(s);
                    if (candidateTriples.contains(Arrays.toString(s))){
                        Integer count = triplesMap.get(Arrays.toString(s));
                        if (count==null){
                            triplesMap.put(Arrays.toString(s),1);
                        }else {
                            triplesMap.put(Arrays.toString(s),count+1);
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        for (Map.Entry <String,Integer> entry: triplesMap.entrySet()) {
            context.write(new Text(entry.getKey()),new IntWritable(entry.getValue()));
        }
        System.out.println("numBaskets in current mapper: "+numBaskets);
        context.write(new Text("numBaskets"),new IntWritable(numBaskets));
    }
}

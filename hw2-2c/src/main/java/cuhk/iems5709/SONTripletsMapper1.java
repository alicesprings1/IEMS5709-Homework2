package cuhk.iems5709;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class SONTripletsMapper1 extends Mapper<LongWritable, Text,Text, NullWritable> {
    int numBaskets=0;
    Map<String,Integer> wordMap=new HashMap<>();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // count number of baskets
        numBaskets++;
        String[] words=value.toString().split(" ");
        // count frequency for each word
        for (String word:words) {
            Integer count = wordMap.get(word);
            if (count==null){
                wordMap.put(word,1);
            }else {
                wordMap.put(word,count+1);
            }
        }
    }
    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        List<String> frequentWords=new ArrayList<>();
        // compute threshold
        int threshold= (int) (0.0025*numBaskets);
        for (Map.Entry <String,Integer> entry:wordMap.entrySet()) {
            String key = entry.getKey();
            Integer value = entry.getValue();
            // filter out infrequent words
            if (value>=threshold){
                frequentWords.add(key);
            }
        }
        // create candidate pairs
        for (int i = 0; i < frequentWords.size(); i++) {
            for (int j = i+1; j < frequentWords.size(); j++) {
                String[] s={frequentWords.get(i), frequentWords.get(j)};
                Arrays.sort(s);
                context.write(new Text(Arrays.toString(s)),NullWritable.get());
            }
        }
    }
}

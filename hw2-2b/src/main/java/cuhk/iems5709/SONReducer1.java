package cuhk.iems5709;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class SONReducer1 extends Reducer<Text, NullWritable,Text, NullWritable> {
    Set<String> frequentWords=new HashSet<>();
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        frequentWords.add(key.toString());
    }

    @Override
    protected void cleanup(Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // generate and store candidate pairs
        List<String> words=new ArrayList<>(frequentWords);
        for (int i = 0; i < words.size(); i++) {
            for (int j = i+1; j < words.size(); j++) {
                String[] s={words.get(i), words.get(j)};
                Arrays.sort(s);
                context.write(new Text(Arrays.toString(s)),NullWritable.get());
            }
        }
    }
}

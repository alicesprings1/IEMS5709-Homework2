package cuhk.iems5709;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SONReducer2 extends Reducer<Text, IntWritable,Text,IntWritable> {
    Map<String,Integer> totalPairsMap=new HashMap<>();
    int totalNumBaskets=0;
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int totalCount=0;
        if (key.toString().equals("numBaskets")){
            for (IntWritable value:values) {
                totalNumBaskets+=value.get();
            }
        }else {
            for (IntWritable value:values) {
                totalCount+= value.get();
            }
            totalPairsMap.put(key.toString(),totalCount);
        }
    }

    @Override
    protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // compute total threshold
        int threshold=(int) (totalNumBaskets*0.005);
        System.out.println("totalNumBaskets: "+totalNumBaskets);
        System.out.println("threshold: "+threshold);
        for (Map.Entry<String,Integer> entry: totalPairsMap.entrySet()) {
            String pair = entry.getKey();
            Integer count = entry.getValue();
            if (count>=threshold){
                context.write(new Text(pair),new IntWritable(count));
            }
        }
    }
}

package cuhk.iems5709;//AprioriPass1.java
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AprioriPass1 {
    public static void main(String[] args) throws IOException {
        // pass1
        // 1. read the dataset and count frequency for each word
        // count the number of lines(number of baskets)
        long startTime = System.currentTimeMillis();
        int numBaskets=0;
        String path="src/dataset";
        BufferedReader reader;
        String line;
        File directory = new File(path);
        File[] files = directory.listFiles();
        Map<String, Integer> wordMap=new HashMap<>();
        for (File file:files) {
            try {
                reader=new BufferedReader(new FileReader(file));
                while ((line=reader.readLine())!=null){
                    numBaskets++;
                    String[] words=line.split(" ");
                    for (String word:words) {
                        Integer count = wordMap.get(word);
                        if (count==null){
                            wordMap.put(word,1);
                        }else {
                            wordMap.put(word,count+1);
                        }
                    }
                }
                reader.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("numBaskets: "+numBaskets);
        // 2.filter out infrequent words
        // calculate the threshold number
        int threshold= (int) (numBaskets*0.005);
        System.out.println("threshold: "+threshold);
        List<String> frequentWords = new ArrayList<>();
        for (Map.Entry<String,Integer> entry:wordMap.entrySet()) {
            String key = entry.getKey();
            Integer value = entry.getValue();
            if (value>=threshold){
                frequentWords.add(key);
//                System.out.println("key: "+key +" value: "+value);
            }
        }
        System.out.println("numFrequentWords: "+frequentWords.size());
        // store the frequent words into a file
        File outputFile = new File("src/output/frequent_words");
        if (outputFile.createNewFile()){
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
            for (String frequentWord:frequentWords) {
                writer.write(frequentWord);
                writer.newLine();
//                System.out.println(frequentWord);
            }
            writer.close();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("time elapsed for pass1: "+(int)(endTime-startTime)/1000+"s");
    }

}

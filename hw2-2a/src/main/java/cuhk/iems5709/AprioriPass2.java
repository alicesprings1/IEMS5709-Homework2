package cuhk.iems5709;//AprioriPass2.java
import java.io.*;
import java.util.*;

public class AprioriPass2 {
    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();
        // 1.generate candidate pairs
        String path1="src/output/frequent_words";
        String path2="src/dataset";
        String line;
        BufferedReader reader;
        Set<String> candidatePairs=new HashSet<>();
        List<String> frequentWords=new ArrayList<>();
        int numBaskets=0;
        // read output file of pass1
        try {
            Scanner scanner = new Scanner(new File(path1));
            while (scanner.hasNextLine()){
                frequentWords.add(scanner.nextLine());
            }
            scanner.close();
            System.out.println("numFrequentWords: "+frequentWords.size());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        // add candidate pairs
        for (int i = 0; i < frequentWords.size(); i++) {
            for (int j = i+1; j < frequentWords.size(); j++) {
                String[] s={frequentWords.get(i),frequentWords.get(j)};
                Arrays.sort(s);
                candidatePairs.add(Arrays.toString(s));
            }
        }
        System.out.println("numCandidatePairs: "+candidatePairs.size());
        // 2. read dataset again and count frequency of candidate pairs
        File directory = new File(path2);
        File[] files = directory.listFiles();
        Map<String,Integer> pairCount=new HashMap<>();
        for (File file:files) {
            try {
                reader=new BufferedReader(new FileReader(file));
                while ((line=reader.readLine())!=null){
                    numBaskets++;
                    String[] words=line.split(" ");
                    for (int i = 0; i < words.length; i++) {
                        for (int j = i+1; j < words.length; j++) {
                            String[] pair={words[i],words[j]};
                            Arrays.sort(pair);
                            String s = Arrays.toString(pair);
                            if (candidatePairs.contains(s)){
                                Integer count = pairCount.get(s);
                                if (count==null){
                                    pairCount.put(s,1);
                                }else {
                                    pairCount.put(s,count+1);
                                }
                            }
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
        //3. filter out infrequent pairs
        // calculate threshold
        int threshold= (int) (numBaskets*0.005);
        System.out.println("threshold: "+threshold);
        List<String> frequentPairs=new ArrayList<>();
        for (Map.Entry<String,Integer> entry:pairCount.entrySet()){
            if (entry.getValue()>=threshold){
                frequentPairs.add(entry.getKey()+" "+entry.getValue());
            }
        }
        // store the frequent pairs
        File outputFile = new File("src/output/frequent_pairs");
        if (outputFile.createNewFile()){
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
            for (String frequentPair:frequentPairs) {
                writer.write(frequentPair);
                writer.newLine();
//                System.out.println(frequentWord);
            }
            writer.close();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("time elapsed for pass2: "+(int)(endTime-startTime)/1000+"s");
    }
}

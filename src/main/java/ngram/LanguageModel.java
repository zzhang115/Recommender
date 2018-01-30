import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by zzc on 7/24/17.
 */
public class LanguageModel
{
    public static class Map extends Mapper<LongWritable, Text, Text, Text>
    {
        // input: I love big data \t 10
        // output: key = I love big value = data=10
        private int threshold;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException
        {
            Configuration configuration = context.getConfiguration();
            threshold = configuration.getInt("topK", 20);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
//            System.out.println("---key: "+key+" value: "+value);
            String line = value.toString().trim();
            String[] wordsPlusCount = line.split("\t");
            if(wordsPlusCount.length < 2)
            {
                return;
            }
            String[] words = wordsPlusCount[0].split("\\s+");
            int count = Integer.parseInt(wordsPlusCount[1]);
            if(count < threshold)
            {
                return;
            }
            StringBuilder stringBuilder = new StringBuilder();
            for(int i = 0; i < words.length - 1; i++)
            {
                stringBuilder.append(words[i]);
                stringBuilder.append(" ");
            }
            String outputKey = stringBuilder.toString().trim();
            String outputValue = words[words.length - 1] + "=" + count;
//            System.out.println("key: "+outputKey + " value: "+outputValue);
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable>
    {
        private int topK;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException
        {
            Configuration configuration = context.getConfiguration();
            topK = configuration.getInt("topK", 5);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            // key = I love big
            // value = <data=10,girl=100,boy=200...>
            TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
            //<10, <data, baby...>, <100, <apple, htc...>>
            for(Text val : values)
            {
                String value = val.toString().trim();
                String word = value.split("=")[0].trim();
                int count = Integer.parseInt(value.split("=")[1].trim());
//                System.out.println("###"+value);
                if(tm.containsKey(count))
                {
                    tm.get(count).add(word);
                }
                else
                {
                    List<String> list = new ArrayList<String>();
                    list.add(word);
                    tm.put(count, list);
                }
            }
            Iterator<Integer> iterator = tm.keySet().iterator();
            for(int j = 0; iterator.hasNext() && j < topK;)
            {
                int keyCount = iterator.next();
                List<String> words = tm.get(keyCount);
                for(int i = 0; i < words.size() && j < topK; i++)
                {
                    context.write(new DBOutputWritable(key.toString(), words.get(i), keyCount), NullWritable.get());
//                    System.out.println("key: "+key.toString()+" value: "+words.get(i)+" count: "+keyCount);
                    j++;
                }
            }
        }
    }
}

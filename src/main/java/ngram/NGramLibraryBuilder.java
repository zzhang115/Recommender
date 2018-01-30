import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Created by zzc on 7/22/17.
 */
public class NGramLibraryBuilder
{
    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private int numGram;
        //setup function only be called one time, just to setup related data
        @Override
        protected void setup(Context context) throws IOException, InterruptedException
        {
            Configuration conf = context.getConfiguration();
            numGram = conf.getInt("nGram", 5);// 5 is default value, if get value is null
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            //input a sentence
            //I love bigdata n = 3
            /* I love -> 1
            love bigdata -> 1
            I love big -> 1
            */
            System.out.println("line:"+value.toString());
            String line = value.toString().trim().toLowerCase().replaceAll("[^a-z]", " ");
            String[] words = line.split("\\s+");
            if(words.length < 2)
            {
                return;
            }
            StringBuilder stringBuilder;
            for(int i = 0; i < words.length; i++)
            {
                stringBuilder = new StringBuilder();
                stringBuilder.append(words[i]);
                for(int j = 1; j < numGram && (i + j) < words.length; j++)
                {
                    stringBuilder.append(" ");
                    stringBuilder.append(words[i + j]);
                    context.write(new Text(stringBuilder.toString()), new IntWritable(1));
                }
//                System.out.println("buf: " + stringBuilder.toString());
            }
        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
//            System.out.println("key: "+key);
            int sum = 0;
            for(IntWritable value : values)
            {
//                System.out.println("value: "+value);
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}

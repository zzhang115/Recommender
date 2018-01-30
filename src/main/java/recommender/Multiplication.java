import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zzc on 8/6/17.
 */

public class Multiplication {
    final static Logger logger = Logger.getLogger(Multiplication.class);

    public static class CocurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            context.write(new Text(line.split("\t")[0]), new Text(line.split("\t")[1]));
        }
    }

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            System.out.println("line:"+line);
            context.write(new Text(line.split(",")[1]), new Text(line.split(",")[0] + "," + line.split(",")[2]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> relationMap = new HashMap<String, Double>();
            Map<String, Double> ratingsMap = new HashMap<String, Double>();
            System.out.println("multiply");
            for (Text value : values) {
                if (value.toString().contains(":")) {
                    System.out.println(value.toString().split(":")[0]+"___"+Double.parseDouble(value.toString().split(":")[1]));
                    relationMap.put(value.toString().split(":")[0], Double.parseDouble(value.toString().split(":")[1]));
                } else {
                    System.out.println(value.toString().split(",")[0]+",,,"+Double.parseDouble(value.toString().split(",")[1]));
                    ratingsMap.put(value.toString().split(",")[0], Double.parseDouble(value.toString().split(",")[1]));
                }
            }
            for (Map.Entry<String, Double> entry : relationMap.entrySet()) {
                String movie = entry.getKey();
                Double relation = entry.getValue();
                for (Map.Entry<String, Double> entry1 : ratingsMap.entrySet()) {
                    String userId = entry1.getKey();
                    Double rating = entry1.getValue();
                    context.write(new Text(userId + ":" + movie), new DoubleWritable(relation * rating));
                    System.out.println(userId + ":" + movie+"relation:"+relation + "rating:"+rating + relation * rating);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String coCurrenceInputFileDir = args[0];
        String ratingsInputFileDir = args[1];
        String outputFileDir = args[2];

        File output4 = new File(outputFileDir);
        if (output4.exists()) {
            if (logger.isInfoEnabled()) {
                logger.info("Output4 directory already exits!\tDelete previous directory.");
            }
            FileUtils.deleteDirectory(output4);
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(Multiplication.class);

        ChainMapper.addMapper(job, CocurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, configuration);
        ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, configuration);

        job.setMapperClass(CocurrenceMapper.class);
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(MultiplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(coCurrenceInputFileDir), TextInputFormat.class, CocurrenceMapper.class);
        MultipleInputs.addInputPath(job, new Path(ratingsInputFileDir), TextInputFormat.class, RatingMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(outputFileDir));
        job.waitForCompletion(true);
    }
}

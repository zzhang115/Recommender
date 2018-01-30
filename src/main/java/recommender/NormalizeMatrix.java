import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zzc on 8/4/17.
 */

public class NormalizeMatrix {
    final static Logger logger = Logger.getLogger(NormalizeMatrix.class);

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("norm"+value.toString());
            String[] array = value.toString().trim().split("-");
            String movieId0 = array[0];
            String movieId1_count = array[1];
            context.write(new Text(movieId0), new Text(movieId1_count));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            Map<String, Integer> map = new HashMap<String, Integer>();
            while (values.iterator().hasNext()) {
                String movie_count = values.iterator().next().toString();
                String movie1 = movie_count.split("\t")[0];
                int subCount = Integer.parseInt(movie_count.split("\t")[1]);
                sum += subCount;
                map.put(movie1, subCount);
            }
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                context.write(key, new Text(entry.getKey() + ":" + (double) entry.getValue() / sum));
            }
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputFileDir = args[0];
        String outputFileDir = args[1];

        File output3 = new File(outputFileDir);
        if (output3.exists()) {
            if (logger.isInfoEnabled()) {
                logger.info("Output3 directory already exits!\tDelete previous directory.");
            }
            FileUtils.deleteDirectory(output3);
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(inputFileDir));
        TextOutputFormat.setOutputPath(job, new Path(outputFileDir));
        job.waitForCompletion(true);
    }
}

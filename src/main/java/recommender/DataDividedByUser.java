import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

/**
 * Created by zzc on 8/4/17.
 */
public class DataDividedByUser {
    final static Logger logger = Logger.getLogger(DataDividedByUser.class);

    public static class DataDivideMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String values[] = value.toString().trim().split(",");
            int userId = Integer.parseInt(values[0]);
            String movieId = values[1];
            String rating = values[2];
            context.write(new IntWritable(userId), new Text(movieId + "-" + rating));
        }
    }

    public static class DataDivideReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer buffer = new StringBuffer();
            for (Text value : values) {
                buffer.append(value + ",");
            }
            buffer.deleteCharAt(buffer.length() - 1);
            System.out.println(key+" : "+buffer.toString());
            context.write(key, new Text(buffer.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputDataDir = args[0];
        String outputDataDir = args[1];

        File output1 = new File(outputDataDir);
        if (output1.exists()) {
            if (logger.isInfoEnabled()) {
                logger.info("Output1 directory already exits!\tDelete previous directory.");
            }
            FileUtils.deleteDirectory(output1);
        }

        Configuration conf= new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(DataDivideMapper.class);
        job.setReducerClass(DataDivideReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(inputDataDir));
        TextOutputFormat.setOutputPath(job, new Path(outputDataDir));
        job.waitForCompletion(true);
    }
}

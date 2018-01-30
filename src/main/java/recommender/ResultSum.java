import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
 * Created by zzc on 8/7/17.
 */

public class ResultSum {
    final static Logger logger = Logger.getLogger(NormalizeMatrix.class);

    public static class ResultSumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] array = value.toString().trim().split("\t");
            context.write(new Text(array[0]), new DoubleWritable(Double.parseDouble(array[1])));
        }
    }

    public static class ResultSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputFileDir = args[0];
        String outputFileDir = args[1];

        File output5 = new File(outputFileDir);
        if (output5.exists()) {
            if (logger.isInfoEnabled()) {
                logger.info("Output5 directory already exits!\tDelete previous directory.");
            }
            FileUtils.deleteDirectory(output5);
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setMapperClass(ResultSumMapper.class);
        job.setReducerClass(ResultSumReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.setInputPaths(job, new Path(inputFileDir));
        TextOutputFormat.setOutputPath(job, new Path(outputFileDir));
        job.waitForCompletion(true);
    }
}

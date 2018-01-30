import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Created by zzc on 7/29/17.
 */
public class UnitSum
{
    public static class SumMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String flow = line.split("\t")[0].trim();
            double UnitValue = Double.parseDouble(line.split("\t")[1].trim());
            context.write(new Text(flow), new DoubleWritable(UnitValue));
        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double valueSum = 0;
            for(DoubleWritable value : values) {
                valueSum += value.get();
            }
            DecimalFormat df = new DecimalFormat("#.0000");
            valueSum = Double.valueOf(df.format(valueSum));
            context.write(new Text(key), new DoubleWritable(valueSum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path unitInputPath = new Path(args[0]);
        Path unitOutputPath= new Path(args[1]);

        File file = new File(args[1]);
        if (file.exists()) {
            System.out.println("Output2 directory already exists\nDelete previous output directory.");
            FileUtils.deleteDirectory(file);
        }
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(UnitSum.class);

        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, unitInputPath);
        FileOutputFormat.setOutputPath(job, unitOutputPath);
        job.waitForCompletion(true);
    }
}

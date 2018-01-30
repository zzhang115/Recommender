import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zzc on 7/29/17.
 */
public class UnitMultiplication {
    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] flowTo = line.split("\t");
            String flow = flowTo[0].trim();
            String[] tos = flowTo[1].trim().split(",");

            if (flowTo.length ==1 || flowTo[1].trim().equals("")) {
               return;
            }
            for (String to : tos) {
                System.out.println("trans--key:"+flow+" value:"+(double) 1 / tos.length);
                context.write(new Text(flow), new Text(to + "=" + (double) 1 / tos.length));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] pr = value.toString().trim().split("\t");
            System.out.println("pr--key:"+key+" value:"+pr[1]);
            context.write(new Text(pr[0]), new Text(pr[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {
        float beta;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            beta = configuration.getFloat("beta", 0.2f);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //key = 1 value = <2=1/4, 7=1/4, 8=1/4, 29=1/4, 1/6012>
            //separate transition cell from pr cell
            //multiply
            List<String> transitionCell = new ArrayList<String>();
            double prUnit = 0;
            System.out.println("key:"+key);
            for (Text value : values) {
                System.out.println(value);
                if (value.toString().contains("=")) {
                    transitionCell.add(value.toString().trim());
                } else {
                    prUnit = Double.parseDouble(value.toString().trim());
                }
            }
            for (String cell : transitionCell) {
                String outputKey = cell.split("=")[0];
                double relation = Double.parseDouble(cell.split("=")[1]);
                String outputValue = String.valueOf(relation * prUnit * (1 - beta));
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path transitionFilePath = new Path(args[0]);
        Path prFilePath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        float beta = Float.parseFloat(args[3]);

        Configuration configuration = new Configuration();
        configuration.setFloat("beta", beta);
        Job job = Job.getInstance(configuration);
        job.setJarByClass(UnitMultiplication.class);

        File file = new File(args[2]);
        if (file.exists()) {
            System.out.println("Output1 directory already exits!\nDelete previous output directory.");
            FileUtils.deleteDirectory(file);
        }

        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, configuration);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, configuration);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, transitionFilePath, TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, prFilePath, TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
    }
}

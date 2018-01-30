import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * Created by zzc on 7/22/17.
 */
public class Driver
{
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException
    {
        String inputDir = args[0];
        String outputDir = args[1];
        String nGrams= args[2];
        String threshold = args[3];
        String topK = args[4];

        File file = new File(outputDir);
        if(file.exists())
        {
            System.out.println("Output directory already exits, delete it.");
            FileUtils.deleteDirectory(file);
        }
        // job1
        Configuration configuration1 = new Configuration();
        configuration1.set("textinputformat.record.delimiter", ".");
        configuration1.set("nGram", nGrams);

        Job job1 = Job.getInstance(configuration1);
        job1.setJobName("NGram");
        job1.setJarByClass(Driver.class);

        job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
        job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job1, new Path(inputDir));
        TextOutputFormat.setOutputPath(job1, new Path(outputDir));
        job1.waitForCompletion(true);

        // connect two jobs, last output is the next input
        // job2
        Configuration configuration2 = new Configuration();
        configuration2.set("threshold", threshold);
        configuration2.set("topK", topK);

        // dburl: mysql ip address and database name, username, password
        DBConfiguration.configureDB(configuration2, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/auto_complete", "root", "238604");
        Job job2 = Job.getInstance(configuration2);
        job2.setJobName("Model");
        job2.setJarByClass(Driver.class);

        FileInputFormat.addInputPath(job2, new Path("mysql-connector-java-5.1.39.jar"));
        job2.setMapperClass(LanguageModel.Map.class);
        job2.setReducerClass(LanguageModel.Reduce.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(DBOutputWritable.class);
        job2.setOutputValueClass(NullWritable.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(DBOutputFormat.class);

        // tableName used to insert data, string[] represents column name in database, must keep same with db
        DBOutputFormat.setOutput(job2, "word_count2", new String[] {"starting_phrase", "following_word", "count"});

        TextInputFormat.setInputPaths(job2, outputDir);
        job2.waitForCompletion(true);
    }
}

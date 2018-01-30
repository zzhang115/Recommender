import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.hash.Hash;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by zzc on 7/29/17.
 */
public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path transitionFilePath = new Path(args[0]);
        Path prFilePath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        float beta = Float.parseFloat(args[3]);
        UnitMultiplication multiplication = new UnitMultiplication();
        UnitSum sum = new UnitSum();

        // example for calling two main methond two class separately
        //args0: dir of transition.txt
        //args1: dir of PageRank.txt
        //args2: dir of unitMultiplication result
        //args3: times of convergence
        String transitionMatrix = args[0];
        String prMatrix = args[1];
        String unitState = args[2];
        int count = Integer.parseInt(args[3]);
        for(int i=0;  i<count;  i++) {
            String[] args1 = {transitionMatrix, prMatrix+i, unitState+i};
            multiplication.main(args1);
            String[] args2 = {unitState + i, prMatrix+(i+1)};
            sum.main(args2);
        }
    }
}

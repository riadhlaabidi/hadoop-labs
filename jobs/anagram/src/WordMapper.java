import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;


public class WordMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String word = value.toString().trim();
        char[] sortedCharArray = word.toLowerCase().toCharArray();
        Arrays.sort(sortedCharArray);
        context.write(new Text(new String(sortedCharArray)), new Text(word));
    }
}
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Set;


public class TweetMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Set<String> negativeWords = Set.of("nul", "insatisfait", "bof", "incompétents");
    private final Set<String> positiveWords = Set.of("satisfait", "super", "excellent");

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] tweet = value
                .toString()
                .trim()
                .toLowerCase()
                .split("\\W+");

        int positive = 0;
        int negative = 0;
        for (String s : tweet) {
            if (positiveWords.contains(s)) {
                positive = 1;
            }
            if (negativeWords.contains(s)) {
                negative = 1;
            }
        }

        Text outputKey = new Text();
        if (positive == 1 && negative == 0) {
            outputKey.set("satisfait");
        } else if (positive == 0 && negative == 1) {
            outputKey.set("insatisfait");
        } else {
            outputKey.set("inconcluant");
        }

        context.write(outputKey, new IntWritable(1));
    }
}
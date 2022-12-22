import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class WordReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Set<String> words = new HashSet<>();
        values.forEach(value -> words.add(value.toString()));
        if (words.size() > 1) {
            String res = StringUtils.join(",", words);
            context.write(key, new Text(res));
        }
    }
}
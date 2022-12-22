import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] line = value.toString().split(":");
		String source = line[0];
		String[] targets = line[1].split(",");
		
		for (String target : targets) {
			context.write(new Text(target), new Text(source));
		}
	}
}

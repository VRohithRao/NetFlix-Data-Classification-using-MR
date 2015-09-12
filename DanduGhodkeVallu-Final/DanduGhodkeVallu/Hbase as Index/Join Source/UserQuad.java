import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class UserQuad {
	// Divides Users in 4 quadrants with number of times user rated and average rankings
	// Takes in DataViz2
	// Number of times rated,rating average 
	// high,high = 1
	// high,low = 2
	// low,high = 3
	// low,low = 4
	public static class UserQuadMap extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] content = line.split("\t");
			String userId = content[0];
			//String movieId = content[1];
			int ratingCount = Integer.parseInt(content[1]);
			float avgRating = Float.parseFloat(content[2]);
			int quad = 0;
			if(ratingCount >= 1130){
				if(avgRating >= 3.5){
					quad = 1;
				}
				if(avgRating < 3.5){
					quad = 2;
				}
				
			}
			if(ratingCount < 1130){
				if(avgRating >= 3.5){
					quad = 3;
				}
				if(avgRating < 3.5){
					quad = 4;
				}
			}
			context.write(new Text(quad+""), new Text(userId));
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: UserQuadMap <in> <out>");
			System.exit(2);
		}
		Job job1 = new Job(conf, "UserQuadMap");
		job1.setJarByClass(MovieQuad.class);
		job1.setNumReduceTasks(0);
		job1.setMapperClass(UserQuadMap.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}

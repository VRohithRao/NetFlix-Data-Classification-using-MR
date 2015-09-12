import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MovieQuad {
	// Divides movies in 4 quadrants with movie number of rankings and average rankings
	// Takes in DataViz1
	// Moviecount,rating 
	// high,high = 1
	// high,low = 2
	// low,high = 3
	// low,low = 4
	public static class MovieQuadMap extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] content = line.split("\t");
			String movieId = content[0];
			int ratingCount = Integer.parseInt(content[4]);
			float avgRating = Float.parseFloat(content[5]);
			String movieTitle = content[6];
			int quad = 0;
			if(ratingCount >= 6000){
				if(avgRating >= 3.5){
					quad = 1;
				}
				if(avgRating < 3.5){
					quad = 2;
				}
				
			}
			if(ratingCount < 6000){
				if(avgRating >= 3.5){
					quad = 3;
				}
				if(avgRating < 3.5){
					quad = 4;
				}
			}
			context.write(new Text(quad+""), new Text(movieId + "\t" + movieTitle));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: MovieQuadMap <in> <out>");
			System.exit(2);
		}
		Job job1 = new Job(conf, "MovieQuadMap");
		job1.setJarByClass(MovieQuad.class);
		job1.setNumReduceTasks(0);
		job1.setMapperClass(MovieQuadMap.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}

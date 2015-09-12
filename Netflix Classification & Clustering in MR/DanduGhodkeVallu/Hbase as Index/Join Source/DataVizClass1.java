import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.kenai.jffi.Array;


public class DataVizClass1 {
	// This class generates: MovieID firstDateRated, lastDateRated, yearOfProduction, 
	// totalNumRatings, avgRating, movieTitle
	// Takes input in2
	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] content = line.split(",");
			String movieId = content[0];
			String rating = content[2];
			String date = content[3];
			context.write(new Text(movieId), new Text("F1;"+rating +";" + date));
		}
	}
	// Takes input in
	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] content = line.split(",");
			String movieId = content[0];
			String yearProd = content[1];
			String title = content[2];
			context.write(new Text(movieId), new Text("F2;"+yearProd+";" + title));
		}
	}
	
	public static class AverageReducer extends
	Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<Integer> ratings = new ArrayList<Integer>();
			ArrayList<String> ratingDate = new ArrayList<String>();
			String yearProd = null;
			String title = null;
			int sum =0;
			for(Text v:values){
				String[] content = v.toString().split(";");
				if(content[0].equals("F1")){
					ratings.add(Integer.parseInt(content[1]));
					sum += Integer.parseInt(content[1]);
					ratingDate.add(content[2]);
				}else{
					yearProd = content[1];
					title = content[2];
				}
			}
			Collections.sort(ratingDate);
			context.write(key,new Text(ratingDate.get(0)+"\t"+ratingDate.get(ratingDate.size()-1) + "\t" + yearProd +"\t" + ratings.size() +"\t" + sum/(float)ratings.size() +"\t" + title));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: DataViz1 <in1> <in2> <out>");
			System.exit(2);
		}
		Job job1 = new Job(conf, "DataViz1");
		job1.setJarByClass(DataVizClass1.class);
		job1.setNumReduceTasks(5);
		MultipleInputs.addInputPath(job1, new Path(otherArgs[0]), TextInputFormat.class, Mapper1.class);
		MultipleInputs.addInputPath(job1, new Path(otherArgs[1]), TextInputFormat.class, Mapper2.class);
		job1.setReducerClass(AverageReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
	
	
}

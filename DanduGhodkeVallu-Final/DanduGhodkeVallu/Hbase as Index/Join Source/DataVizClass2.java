import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class DataVizClass2 {
	// Produces output CustomerID,ratingCount, ratingAvg 
	// movieRatingList is the list of every movie the user has rated
	
	// input should be in2
	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] content = line.split(",");
			String userID = content[1];
			String ratings = content[2];
			String movieId = content[0];
			context.write(new Text(userID), new Text(ratings +";" + movieId));
		}
	}
	
	public static class AverageReducer extends
	Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//System.out.println("In reduce");
			ArrayList<Integer> ratings = new ArrayList<Integer>();
			ArrayList<String> movieID = new ArrayList<String>();
			int sum =0;
			for(Text v:values){
				String[] content = v.toString().split(";");
				sum += Integer.parseInt(content[0]);
				ratings.add(Integer.parseInt(content[0]));
				movieID.add(content[1]);	
			}
			context.write(key, new Text(ratings.size() +"\t" + sum/(float) ratings.size() ));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: DataViz2 <in> <out>");
			System.exit(2);
		}
		Job job1 = new Job(conf, "Data Viz Class 2");
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setJarByClass(DataVizClass2.class);
		job1.setMapperClass(Mapper1.class);
		job1.setNumReduceTasks(5);
		FileInputFormat.setInputPaths(job1, new Path(otherArgs[0]));
		job1.setReducerClass(AverageReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
	
}

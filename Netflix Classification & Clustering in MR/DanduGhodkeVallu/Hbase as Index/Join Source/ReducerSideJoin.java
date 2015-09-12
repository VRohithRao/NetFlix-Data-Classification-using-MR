import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//New import end



import au.com.bytecode.opencsv.CSVParser;


//New import
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.mapred.TextInputFormat;

public class ReducerSideJoin {
	public static class Mapper1 extends
			Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// CSV Parser
			CSVParser csv = new CSVParser(',');

			// Getting the data
			String[] content = csv.parseLine(value.toString());

			String movieId = content[0];
			String movieTitle = content[2];
			context.write(new Text(movieId), new Text(movieTitle + ";"
					+ "F1"));
		}
	}

	public static class Mapper2 extends
			Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// CSV Parser
			CSVParser csv = new CSVParser(',');

			// Getting the data
			String[] content = csv.parseLine(value.toString());

			String movieId = content[0];
			String Rating = content[2];
			String ratingDate = content[3];
			String date[] = ratingDate.split("-");
			String ratingYear = date[0];
			//System.out.println("Map2");
			context.write(new Text(movieId), new Text(Rating + ";"
					+ "F2;" + ratingYear));
		}
	}

	public static class AverageReducer extends
			Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			//ArrayList<Integer> ratings = new ArrayList<Integer>();
			HashMap<Integer, Integer> sum = new HashMap<Integer,Integer>();
			HashMap<Integer, Integer> count = new HashMap<Integer,Integer>();
			String name = null;
			for(Text v:values){
				String Value = v.toString();
				String[] content = Value.split(";");
				if(content[1].equals("F1")){
					name = content[0];
				}
				if(content[1].equals("F2")){
					if(sum.containsKey(Integer.parseInt(content[2]))){
						sum.put(Integer.parseInt(content[2]), sum.get(Integer.parseInt(content[2])) + Integer.parseInt(content[0]));
						count.put(Integer.parseInt(content[2]), count.get(Integer.parseInt(content[2])) + 1);
					}
					else{
						sum.put(Integer.parseInt(content[2]), Integer.parseInt(content[0]));
						count.put(Integer.parseInt(content[2]), 1);
					}
				}
			}
			for(Integer k:sum.keySet()){
				float output = sum.get(k)/(float)count.get(k);
				context.write(new Text(name), new Text(k +"" + "\t" + output+""));
			}
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: ReducerSideJoin <in1> <in2> <out>");
			System.exit(2);
		}
		Job job1 = new Job(conf, "ReducerSideJoin");
		job1.setJarByClass(ReducerSideJoin.class);
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

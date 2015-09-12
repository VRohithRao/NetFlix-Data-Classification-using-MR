import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Repl{
	public static class AverageMapper extends Mapper<Object, Text, Text, Text> {
		static HashMap<Integer, String> MovieMap = new HashMap<Integer, String>();
		BufferedReader br;
		String MovieName = "";
		Text outKey = new Text("");
		Text outValue = new Text("");
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
	 
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path path : cacheFiles) {
				loadMap(path, context);
			}
	 
		}
	 
		private void loadMap(Path filePath, Context context)
				throws IOException {
			String strLineRead = "";
	 
			try {
				br = new BufferedReader(new FileReader(filePath.toString()));
				while ((strLineRead = br.readLine()) != null) {
					String movieData[] = strLineRead.split(",");
					MovieMap.put(Integer.parseInt(movieData[0]),
							movieData[2]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
	 
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
				String CorrectData[] = value.toString().split(",");
				MovieName = MovieMap.get(Integer.parseInt(CorrectData[0]));
				outKey.set(MovieName);
				String Rating = CorrectData[2];
				String ratingDate = CorrectData[3];
				String date[] = ratingDate.split("-");
				String ratingYear = date[0];
				outValue.set(Rating+";" + ratingYear);
				context.write(outKey, outValue);
		}
		
	}
	
	// Defining the reducer to get the Average per year. 
	
	public static class AverageReducer extends
	Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException {
			HashMap<Integer, Integer> sum = new HashMap<Integer,Integer>();
			HashMap<Integer, Integer> count = new HashMap<Integer,Integer>();
			for(Text v:values){
				String Value = v.toString();
				String[] content = Value.split(";");
				if(sum.containsKey(Integer.parseInt(content[1]))){
					sum.put(Integer.parseInt(content[1]), sum.get(Integer.parseInt(content[1])) + Integer.parseInt(content[0]));
					count.put(Integer.parseInt(content[1]), count.get(Integer.parseInt(content[1])) + 1);
				}
				else{
					sum.put(Integer.parseInt(content[1]), Integer.parseInt(content[0]));
					count.put(Integer.parseInt(content[1]), 1);
				}
			}
			for(Integer k:sum.keySet()){
				float output = sum.get(k)/(float)count.get(k);
				context.write(key, new Text(k +"" + "\t" + output+""));
			}
		}
	}
	
	
	
	
	public static class DriverMapSideJoinDCacheTxtFile extends Configured implements Tool {
			public int run(String[] args) throws Exception {
		 
				if (args.length != 2) {
					System.out.printf("Two parameters are required- <input dir> <output dir>\n");
					return -1;
				}
		 
				Job job = new Job(getConf());
				Configuration conf = job.getConfiguration();
				job.setJarByClass(Repl.class);
				job.setJobName("Map-side join with text lookup file in DCache");
				DistributedCache.addCacheFile(new URI("s3n://projectdataaniket/movie_titles.txt"),conf);
				//DistributedCache.addCacheFile(new URI("/Users/Aniket/Documents/WorkSpace_New/ProjectFinal/in/movie_titles.txt"),conf);
				job.setJarByClass(DriverMapSideJoinDCacheTxtFile.class);
				FileInputFormat.setInputPaths(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				job.setMapperClass(AverageMapper.class);
				job.setReducerClass(AverageReducer.class);
				job.setNumReduceTasks(5);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				
				boolean success = job.waitForCompletion(true);
				return success ? 0 : 1;
			}
		 
		}
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),
				new DriverMapSideJoinDCacheTxtFile(), args);
		System.exit(exitCode);
	}
}


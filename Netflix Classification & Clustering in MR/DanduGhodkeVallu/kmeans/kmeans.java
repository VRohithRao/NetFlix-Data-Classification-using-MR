import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
//import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapred.job
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.*;



@SuppressWarnings("deprecation")
public class kmeans{
	//public static String centre_path = "/home/bharat/Downloads/Project/Kmeans/";
	//public static String res = "res";
	public static int l=0;
	//public static Double centroid_previous [][] = new Double[][] {{0.0,0.0},{0.0,0.0},{0.0,0.0},{0.0,0.0},{0.0,0.0}};
	//public static int check=0;
	
	//Mapper where we find the distances and send it to reducers
	public static class kmeansMapper extends Mapper<Object,Text,Text,Text>{
				
		public static Double centroid[][];
		boolean[] centroid_to_reduce_check = new boolean[] {false,false,false,false,false};
		Text closest_centre = new Text();
		
		//Read the centroids from the distributed cache files
		@SuppressWarnings("null")
		public void setup(Context context){
			 try{
				 centroid = new Double[5][2];
				 String result_folder = Integer.toString(l);
				 l=l+1;
                 int x=0;
                 Path uris[]={new Path("a"),new Path("b"),new Path("c"),new Path("d"),new Path("e")};;
                 int i=0;
                 String new_filename;
                 while(x<5){//no.of reducers
                	 uris[x] = DistributedCache.getLocalCacheFiles(context.getConfiguration())[x];
                	 new_filename = "part-r-0000" + Integer.toString(x);
                	 if(uris[x].getName().toString().equals(new_filename)){
                		 //Donot use file system as it doe snot extract the file names on AWS with the following approach
                		 //FileSystem fs = FileSystem.get(new Configuration());
                         //BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(uris[x])));
                		 BufferedReader br=new BufferedReader(new FileReader(uris[x].toString()));
                		 String line;
                         line=br.readLine();
                         CSVParser parser = new CSVParser();
                         String coordinates[] = new String[2]; 
                         
                         
                         while (line != null && !line.isEmpty()){
                               coordinates = parser.parseLine(line);
                               centroid[i][0] = Double.parseDouble(coordinates[0]);
                               centroid[i][1] = Double.parseDouble(coordinates[1]);
                               line=br.readLine();
                               i=i+1;
                         }
                	 }
                	 else{
                		 System.out.println(uris[x].toString());
                	 }
                	 
                     x++;
                 }
                
                 
                 
			 }catch(Exception e){
				 System.out.println(e);
			 }
			 
		}
		
		//Input:Each line from the movie dataset as value with an index as key
		//Output:The centroid that is closest to the each movie record as key and the record as value
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			if(value!=null || !value.equals("")){
				CSVParser parser = new CSVParser('\t');
				String movies[] = new String[10];//movie,year,avg-rating
				movies = parser.parseLine(value.toString());
				double distance = Double.POSITIVE_INFINITY;
				int i=0;
				double current_distance;
				int length = movies.length;
				
				Double new_centre[][] = new Double[1][2];
				int k=0;
				while(i<5){
					if(centroid[i][0]!=null && centroid[i][0]!=null){
						current_distance = Math.sqrt(Math.pow((centroid[i][0]-Double.parseDouble(movies[length-2])),2) + 
								Math.pow((centroid[i][1]-Double.parseDouble(movies[length-1])),2));
						
						if(distance>current_distance){
							distance = current_distance;
							new_centre[0][0] = centroid[i][0];
							new_centre[0][1] = centroid[i][1];
							k=i; 
							
						}	
					}
					
					i++;
				}
				if(new_centre[0][0]!=null && new_centre[0][1]!=null){
					centroid_to_reduce_check[k] = true;
					String distance_string = Double.toString(new_centre[0][0]) + "," + Double.toString(new_centre[0][1]);//String.valueOf(distance);
					closest_centre.set(distance_string);
					context.write(closest_centre, value);
				}
				
							
			}			
		}
		//Checking if data was sent to all reducers not so that reduce files are created for next iterations to read all input files as shown in setup
		public void cleanup(Context context) throws IOException, InterruptedException{
			Integer i=0;
			while(i<5){
				
				if(centroid_to_reduce_check[i]!=true && centroid[i][0]!=null && centroid[i][0]!=null){
					String distance_string = Double.toString(centroid[i][0]) + "," + Double.toString(centroid[i][1]);
					closest_centre.set(distance_string);
					Text value = new Text();
					context.write(closest_centre,value);
				}
				i++;
			}
		}
	}
	
	
	//Input:Output of Map call
	//Output:Same as Input
	//Description:Range partitioning is done based on year of rating of movie
	public static class kmeansPartitioner extends Partitioner<Text,Text>{
    	
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			 //TODO Auto-generated method stub
			CSVParser partition_parser = new CSVParser(',');
			String year_rating[] = new String[2];
			Double yearStar[] = new Double[2];
			Integer i=0;
			try {
				year_rating = partition_parser.parseLine(key.toString());
				yearStar[0]=Double.parseDouble(year_rating[0]);
				yearStar[1]=Double.parseDouble(year_rating[1]);
				if(yearStar[0]>=1996.0 && yearStar[0]<1998.0){
					i=0;
				}
				if(yearStar[0]>=1998.0 && yearStar[0]<2000.0){
					i=1;
				}
				if(yearStar[0]>=2000.0 && yearStar[0]<2002.0){
					i=2;
				}
				if(yearStar[0]>=2002.0 && yearStar[0]<2003){
					i=3;
				}
				if(yearStar[0]>=2003.0){
					i=4;
				}
				else i=0;
				//5 is number of reducers
			} catch (Exception e) {
				// TODO Auto-generated catch block
				System.out.println(e);
			}
			return (i%numPartitions);
		}
	}
	
	//Here we find the new centroids and write them into a file
	public static class  kmeansReducer extends Reducer<Text, Text, Text, Text>{
		Text centroid = new Text();
		Text nothing = new Text();
		@Override
		//Input:The centroid as key and list of movies closest to the centroids as value;
		//Output:New centroids
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			CSVParser parser = new CSVParser('\t');
			String movie_record[] = new String[10];
			Double total_count=0.00;
			Double year_total=0.00;
			Double rating_total=0.00;
			Double average_year;
			Double average_rating;
			int length; 
			
			{
				for(Text value:values){
					movie_record = parser.parseLine(value.toString());
					length = movie_record.length;
					if(movie_record[0].isEmpty()){
						
						CSVParser parser_NaN = new CSVParser();
						String keys[] = new String[10];
						keys = parser_NaN.parseLine(key.toString());
							
						year_total = Double.parseDouble(keys[0]);
						rating_total += Double.parseDouble(keys[1]);
						total_count = 1.00;
						continue;
							
					}
					year_total += Double.parseDouble(movie_record[length-2]);
					rating_total += Double.parseDouble(movie_record[length-1]);
					total_count +=1.00;
				}
				average_year = year_total/total_count;
				average_rating = rating_total/total_count;
				String centre = Double.toString(average_year) + "," + Double.toString(average_rating);
				centroid.set(centre);
				
				
				context.write(centroid, nothing);
			}
			
			
		}
		public void cleanup(Context context){
			//context.getCounter(MoreIterations.numberOfIterations).increment(1L);
			//System.out.println(MoreIterations.numberOfIterations);
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Integer iterations = 0;
		int i=1;
		int j=0;
		String input;
		String output = null;
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: PageRank <in> <out> <iterations> <res>");
			System.exit(2);
		}
		
		input = otherArgs[0];
		output = otherArgs[1];
		
		iterations = Integer.parseInt(otherArgs[2]);
		
		while(i<iterations){//just iterating since convergence takes a long time
			output = otherArgs[1]+i;
			Job job = new Job(conf, "K-Means");
			
			
			job.setJarByClass(kmeans.class);
			job.setMapperClass(kmeansMapper.class);
	        job.setPartitionerClass(kmeansPartitioner.class);
	        job.setReducerClass(kmeansReducer.class);	    
			job.setNumReduceTasks(5);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			Path cache[] = {new Path("a"),new Path("b"),new Path("c"),new Path("d"),new Path("e")};
			Date date = new Date();
			try{
				int y=0;
				while(y<5){//number of reduce calls
					cache[y] = new Path(args[3] +j+ "/part-r-0000" + Integer.toString(y));
					System.out.println(date.toString());
					DistributedCache.addCacheFile(cache[y].toUri(), job.getConfiguration());
					System.out.println(date.toString());
					System.out.println(args[3] +j+ "/part-r-0000" + Integer.toString(y));
					String pt = args[3] +j+ "/part-r-0000" + Integer.toString(y);
					//DistributedCache.setLocalFiles(job.getConfiguration(), pt);
					
					y++;
				}
				
			}
			catch(Exception e){
				System.out.println(e);
			}
			
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			
			
			i++;
			j++;
			job.waitForCompletion(true);
		}
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

	
}

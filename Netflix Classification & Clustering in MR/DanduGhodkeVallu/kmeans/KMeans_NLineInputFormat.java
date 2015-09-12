import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
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

//An iterative approach where the converged centroid is found in just one map call
//Flaw: For a very big data file the worker machine keeps iterating and does not 
//respond to the master machine which results in map task attempt failed due to no
//response from mapper since 600 seconds

enum MoreIterations {
        converge;
    }


//Approach of kmeans clustering using map only job and number of configurations.
//Here each map call performs k-means clustering on a single configuration just
//like the way a local machine does it
@SuppressWarnings("deprecation")
public class KMeans_NLineInputFormat{
	public static String centre_path = "/home/bharat/Downloads/Project/Kmeans/";
	public static String res = "res";
	public static int l=0;
	public static Double centroid_previous [][] = new Double[][] {{0.0,0.0},{0.0,0.0},{0.0,0.0},{0.0,0.0},{0.0,0.0}};
	public static int check=0;
	
	public static class kmeansMapper extends Mapper<Object,Text,Text,Text>{
		Double centroid[][];
		Double centroid_new[][]; 
		Text all_centers = new Text();
		boolean[] centroid_to_reduce_check = new boolean[] {false,false,false,false,false};
		Text closest_centre = new Text();
		String all_centroids=null;
		
		Path uris[];
		//Loading the entire movie data file into the distributed cache
		@SuppressWarnings("null")
		public void setup(Context context){
			 try{
				 centroid = new Double[5][2];
				 Date date = new Date();
				 System.out.println(date.toString());
				 uris= DistributedCache.getLocalCacheFiles(context.getConfiguration());
				 System.out.println(date.toString());
			 }
			 catch(Exception e){
				 System.out.println(e);
			 }
		}
		
		//Input: Takes one of the configurations for k-means from the input file
		//Output:Returns the new centroids after convergence
		//Description:NLineInputFormat sends each configuration to a different map call
		//which results in each map call performing k-means clustering on a configuration given
		//and finally giving output on convergence
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			Counter counter = context.getCounter(MoreIterations.converge);
			Boolean q = false;
			int d=0;
			int length;
			if(value!=null || !value.equals("")){
				CSVParser parser1 = new CSVParser(';');
				String[] centers = new String[10];
				centers = parser1.parseLine(value.toString());
				length = centers.length;
				int w=0;
				CSVParser parser2 = new CSVParser(',');
				centroid = new Double[5][2];
				String[] points = new String[2];
				for(w=0;w<length;w++){
					points = parser2.parseLine(centers[w]);
					centroid[w][0] = Double.parseDouble(points[0]);
					centroid[w][1] = Double.parseDouble(points[1]);	
					
				}
			}
			int h=0;
			while(q!=true || h<5){
				//if(value!=null || !value.equals("")){
					
					
					Double[][] yearRatingTotalCount = {{0.0,0.0,0.0},{0.0,0.0,0.0},{0.0,0.0,0.0},{0.0,0.0,0.0},{0.0,0.0,0.0}};
					CSVParser parser3 = new CSVParser('\t');
					String[] movies = new String[10];
					Date date = new Date();
					System.out.println(date.toString());
					BufferedReader br=new BufferedReader(new FileReader(uris[0].toString()));
					System.out.println(date.toString());
	       		 	String line;
	                line=br.readLine();
	                
	                double distance = Double.POSITIVE_INFINITY;
	                double current_distance;
	                int u;
	                int k=0;
	                while (line != null && !line.isEmpty()){
	                	movies= parser3.parseLine(line);
	                	length = movies.length;
	                	for(u=0;u<5;u++){//number of centroids
	                		//if(centroid[u][0]!=null && centroid[u][0]!=null){
	                			current_distance = Math.sqrt(Math.pow((centroid[u][0]-Double.parseDouble(movies[length-2])),2) + 
	    								Math.pow((centroid[u][1]-Double.parseDouble(movies[length-1])),2));
	                    		if(distance>current_distance){
	                    			distance = current_distance;
	                    			k=u;
	                    		}
	                		//}
	                		
	                	}
	                	distance = Double.POSITIVE_INFINITY;
	                	yearRatingTotalCount[k][0] += Double.parseDouble(movies[length-2]);
	                	yearRatingTotalCount[k][1] += Double.parseDouble(movies[length-1]);
	                	yearRatingTotalCount[k][2] += 1.0;
	                	line=br.readLine();
	                }
	                
	                for(u=0;u<5;u++){
	                	if(yearRatingTotalCount[u][0]==0.0 || yearRatingTotalCount[u][1]==0.0){
	                		yearRatingTotalCount[u][0]=centroid[u][0];
	                		yearRatingTotalCount[u][1]=centroid[u][1];
	                		yearRatingTotalCount[u][2]=1.0;
	                	}
	                }
	                centroid_new = new Double[5][2];//used for convergence later
	                int t;
	                String year;
	                String star;
	                String year_star;
	                
	                
	                for(t=0;t<5;t++){
	                	year = Double.toString(yearRatingTotalCount[t][0]/yearRatingTotalCount[t][2]);
	                	star = Double.toString(yearRatingTotalCount[t][1]/yearRatingTotalCount[t][2]);
	                	centroid_new[t][0] = Double.parseDouble(year);
	                	centroid_new[t][1] = Double.parseDouble(star);
	                	year_star = year + "," + star;
	                	if(t==0){
	                		all_centroids = year_star;
	                	}
	                	else{
	                		all_centroids = all_centroids + ";" + year_star;
	                	}
	                	
	                }
	                
	                //Check for convergence till 2nd decimal
	                int s;
	                DecimalFormat df = new DecimalFormat("#.##");
	                for(s=0;s<5;s++){
	                	if(df.format(centroid_new[s][0]).equals(df.format(centroid[s][0])) && 
	                			df.format(centroid_new[s][1]).equals(df.format(centroid[s][1]))){
	                		
	                		if(s==4){
	                			context.getCounter(MoreIterations.converge).increment(1);
	                			q = true;
	                		}
	                		
	                	}
	                	else{
	                		s=0;
	                		break;
	                	}
	                	
	                }
	                int r;
	                for(r=0;r<5;r++){
	                	centroid[r][0] = centroid_new[r][0];
                		centroid[r][1] = centroid_new[r][1];
	                }
	                
					
				//}
				d++;
				System.out.println(d);
				h++;
				
			}
			all_centers.set(all_centroids);
            Text nothing = new Text();
            context.write(all_centers,nothing);
			
					
		}
		
	}
	
	
	
	public static void main(String[] args) throws Exception {
		
		Integer iterations = 0;
		//Path pt=new Path("/home/bharat/Downloads/Project/Kmeans/res/" + MoreIterations.numberOfIterations + "/part-r-00000");
		
		//context.getCounter(MoreIterations.numberOfIterations).increment(1L);
		int i=1;
		int j=0;
		String input;
		String output = null;
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: Kmeans-NLineInputFormat <in> <out> <iterations> <res>");
			System.exit(2);
		}
		//Job job = new Job(conf, "PageRank");
		input = otherArgs[0];
		output = otherArgs[1];
		//output = otherArgs[1];
		iterations = Integer.parseInt(otherArgs[2]);
		
		Path cache = new Path(input);
		Date date = new Date();
		System.out.println(date.toString());
		DistributedCache.addCacheFile(cache.toUri(), conf);
		System.out.println(date.toString());
		
		Counters counter;
		Counter c1 = null;
		//while(c1==null || c1.getValue()!=1.00){//looking for convergence
			output = otherArgs[1]+i;
			Job job = new Job(conf, "K-Means");
			// s3File should be a URI with s3: or s3n: protocol. It will be accessible as a local filed called 'theFile'
	        
			job.setJarByClass(KMeans_NLineInputFormat.class);
			job.setMapperClass(kmeansMapper.class);
			job.setNumReduceTasks(0);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			//The below code is used when map task goes unresponsive for more than 600s 
			//when computing large amount of data. The work around is to write the centroids
			//into a file after each iteration and take that as input so that map task does
			//not go unresponsive for very long
			
			//input = output.concat("/part-r-00000");
			//output = output.concat("/pr1");
			//Path  cache[] = null;
			//Path cache[] = {new Path("a"),new Path("b"),new Path("c"),new Path("d"),new Path("e")};
			/*try{
				int y=0;
				while(y<5){//number of reduce calls
					//String pth = args[3] +j+ "/part-r-0000" + Integer.toBinaryString(y);
					cache[y] = new Path(args[3] +j+ "/part-r-0000" + Integer.toString(y));
					DistributedCache.addCacheFile(cache[y].toUri(), job.getConfiguration());
					System.out.println(args[3] +j+ "/part-r-0000" + Integer.toString(y));
					String pt = args[3] +j+ "/part-r-0000" + Integer.toString(y);
					//DistributedCache.setLocalFiles(job.getConfiguration(), pt);
					
					y++;
				}
				Path data = new Path(input);
				DistributedCache.addCacheFile(data.toUri(), job.getConfiguration());
				
			}
			catch(Exception e){
				System.out.println(e);
			}*/
			//Path cache = new Path(input);
			//DistributedCache.addCacheFile(cache.toUri(), job.getConfiguration());
			//Path cache = new Path("/home/bharat/Downloads/Project/Kmeans/res/" +j+ "/part-r-00000");
			//DistributedCache.addCacheFile(cache.toUri(), job.getConfiguration());
			job.setInputFormatClass(NLineInputFormat.class);
			String pt = args[3] +j+ "/part-m-00000";// + Integer.toString(y);
			NLineInputFormat.addInputPath(job, new Path(pt));
			//FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			
			//input=output;
			//input = output.concat("/part-r-00000");
			//output = output.concat("/pr1");	
			//System.exit(job.waitForCompletion(true) ? 0 : 3);
			i++;
			j++;
			
			if(i==100){
				System.out.println("breaking");
				//break;
			}
			job.waitForCompletion(true);
			counter = job.getCounters();
			c1 = counter.findCounter(MoreIterations.converge);
			
			
		//}
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

	
}

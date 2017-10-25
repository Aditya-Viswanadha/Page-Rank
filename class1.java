package src;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class class1 extends Configured implements Tool{
	
	public static final Pattern titlePattern = Pattern.compile(".*<title>(.*?)</title>.*");
	public static final Pattern textPattern = Pattern.compile(".*<text.*?>(.*?)</text>.*");	
	public static final Pattern linkPattern = Pattern.compile("\\[\\[(.*?)\\]\\]");
	
	public static int tempCount = 1;				//Counting the number of iterations of page rank calculations
	public static double dampingFactor = 0.85;		//Taking the damping factor as 0.85 for calculation of page rank
	public static int max = 20;						// Limiting the maximum number of iterations.
	
	//Main function
	public static void main(String args[]) throws Exception{
		int res = ToolRunner.run(new class1(), args);			//Calling the run function of job 1
		  System.exit(res);
	}
	
	
	//Run function to define all jobs and to configure them.
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		//job1 is the job which extracts info from XML files.
			
		Job job1=Job.getInstance(getConf(),"Rank1");
		job1.setJarByClass(this.getClass());
		//after creating instances, define the input and output formats and set the map and reduce class.
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "Temp/IntermediateOutput" + tempCount));
		//Defining the mapper and reducer classes
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		//Setting the output types for map and reduce class
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		if(job1.waitForCompletion(true)) {			
			ToolRunner.run(new class2(), args);		// Wait until the Job 1 is completed and call the Job2 run function.
			System.out.println("After the wait for completion job2");
		} 
		
		
		
		//System.out.println(tempCount);

		return 0;
	}

	//Map1
	 
		public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {		
			@Override
	        public void map(LongWritable off, Text Text, Context context) throws IOException, InterruptedException {
				
	            String line = Text.toString();					//convert the text coming to string
	            if(line.length() > 0) {
	                Matcher titleMatPat = class1.titlePattern.matcher(line);	//matching the pattern to extract the title of the input file
	                StringBuilder titleBuilder = new StringBuilder();
	                if(titleMatPat.matches()) {
	                    titleBuilder.append(titleMatPat.group(1).toString());
	                }
	                Matcher textMatPat = class1.textPattern.matcher(line);
	                //StringBuilder linkBuilder = new StringBuilder();
	                if(textMatPat.matches()) {                    
	                    String outLinks = textMatPat.group(1).toString();		//matching the pattern to extract the text of the input file
	                    
	                    Matcher linkMatPat = class1.linkPattern.matcher(outLinks);
	                    while(linkMatPat.find()) {
	                        int i = 0;
	                        while(i < linkMatPat.groupCount()) {
	                            
	                            context.write(new Text(linkMatPat.group(i + 1).toString().trim()), new Text(""));
	                            context.write(new Text(titleBuilder.toString().trim()), new Text(linkMatPat.group(i + 1).toString()));//writing the outlinks
	                            i++;
	                        }
	                    }
	                }
	            }
	        }
	    }
		
	//Reduce 1
		public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
			@Override
			public void reduce(Text key, Iterable<Text> Links, Context context) throws IOException, InterruptedException {
	            int outlinks = 0;
	            StringBuilder linkBuilder = new StringBuilder("");
	            for(Text Text : Links) {
	                if(Text.toString().length() > 0) {
	                    linkBuilder.append("#####*****" + Text.toString());//appending some delimeter which we can use for seperating for later stage
	                    outlinks++;
	                }
	            }
	            String val = "Links-----" + outlinks + linkBuilder.toString().trim();
	            context.write(key, new Text(val));
	        }
	    }
}

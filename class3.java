package src;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class class3  extends Configured implements Tool {


	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = getConf();	        
		Job job3 = Job.getInstance(conf, "PRCleanUp");	//Creating a job
		
		conf.set("path", args[1] + "Temp");
		job3.setJarByClass(this.getClass());
		
		FileInputFormat.addInputPath(job3, new Path(args[1] + "Temp/IntermediateOutput" + (class1.tempCount)));// Taking the input from the last iteration output
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		
		job3.setMapperClass(Map3.class);	//Define the mapper class	
		job3.setReducerClass(Reduce3.class);	//Define the reducer class
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
		System.out.println("Before the wait for completion");
		
		return job3.waitForCompletion(true)?0:1;	//Wait for the job completion
		
	}
	
	public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
		@Override
        public void map(LongWritable off, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString();
			if(line.length() > 0) {
                 String[] lineSplit = line.split("\\t");
                 String keyText = lineSplit[0];
                 String[] sectionSplit = lineSplit[1].trim().split("-----");
                 String pageRankVal = sectionSplit[0];
                 context.write(new Text("AdityaIdValue"), new Text(keyText + "#####*****" + pageRankVal));
            }
        }
    }

	public static class Reduce3 extends Reducer<Text, Text, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable<Text> allLinks, Context context) throws IOException, InterruptedException {
			
			Iterator<Text> i = allLinks.iterator();
			
			//FileSystem fs = FileSystem.get(context.getConfiguration());
			// Deleting the intermediate files
			//fs.delete(new Path(intermediatePath),true);
			ArrayList<String> keyArr = new ArrayList<String>();
			
			// move all the text objects into arraylist of type string
			//objects are moved into arraylist
			while (i.hasNext()) {
			    Text currText = i.next();
			    keyArr.add(currText.toString());	
			}
			// split the strings in the arraylist and sort it decreasing order ofpagerank score value
			Collections.sort(keyArr, new Comparator<String>() {
				public int compare(String s1, String s2) {
					String str1[] = s1.split("#####*****");
					String str2[] = s2.split("#####*****");
					double d1 = 0.0;
					double d2 = 0.0;
			       	try {
			       		d1 = Double.parseDouble(str1[1]);
			       		d2 = Double.parseDouble(str2[1]);	
			       		if (d1 > d2) {
			       			return -1;
			       		} else if (d1 < d2) {
			       			return 1;
			       		} else  {
			       			return 0;
			       		}
			       	} catch (Exception E) {
			       		E.printStackTrace();
			       	} 
			       	return 0;
			   }
			});
			// finalKeysArray is now sorted in descending order so write to the context
			String splitArr[];			
			for ( String keyString  : keyArr) {	
				splitArr = keyString.split("#####*****"); 
				context.write(new Text(splitArr[0]), new DoubleWritable(Double.parseDouble(splitArr[1]))); 
			}
        }
    }
}

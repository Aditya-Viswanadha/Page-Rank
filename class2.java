package src;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class class2 extends Configured implements Tool {

	boolean steadyst= false;
	
		
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		//System.out.println("in the run of class2 but before the call of fun pagerankcalc");
		
		pagerankcalc(arg0);	//Calls the function page rank calculations where the page rank is calculated iteratvely
		
		//System.out.println("in the run of class2 after the call of fun pagerankcalc");
		//System.out.println(arg0[0]);
		//System.out.println(arg0[1]);
		
		try {
			ToolRunner.run(new class3(), arg0); //The next class for sorting and cleaning is called after the second job is completed
		} catch (Exception e) {
			// TODO: handle exception
		}
		System.out.println("After the wait for completion of job3");
		return 0;
	}
	
	public void pagerankcalc(String[] args) throws Exception{
	
		System.out.println("in  pagerankcalc");
		while(class1.tempCount<class1.max){			//Loop till max number of iterations are reached or we reach a steady state.
			Configuration conf = getConf();	
		    Job job2;														//Creating a job
			//System.out.println("creating job 0 -- " + class1.tempCount);	
        	job2 = Job.getInstance(conf, "Rank1" + class1.tempCount);
        	//System.out.println("creating job 1");
            job2.setJarByClass(this.getClass());
            //System.out.println("creating job 2");
            
            String inputPath  = args[1] +  "Temp/IntermediateOutput" + class1.tempCount;			//Creating a temporary path for the outputs of individual iterations.
    		String outputPath = args[1] +  "Temp/IntermediateOutput" + (class1.tempCount + 1);
            
    		
    		
    		FileInputFormat.addInputPath(job2, new Path(inputPath));
    		FileOutputFormat.setOutputPath(job2, new Path(outputPath));
    		
    		job2.setMapperClass(Map2.class);			//Setting the mapper class 
    		job2.setReducerClass(Reduce2.class);		//Setting the reducer class
    		
    		job2.setOutputKeyClass(Text.class);
    		job2.setOutputValueClass(Text.class);
    		
    		job2.waitForCompletion(true);	
    				
    		File file1=new File(inputPath + "/part-r-00000");	//Comparing the immediate output files of iterations, if there is no change, the pagerank values are static and we need not continue iterations.
    		File file2=new File(outputPath + "/part-r-00000");
    		if(FileUtils.contentEquals(file1, file2))
    			break;
    		
    		class1.tempCount++;
        
        
        }
		
		
	}


	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
	
		public void map(LongWritable off, Text Text1, Context context) throws IOException, InterruptedException {
			try {
                String line = Text1.toString();
                //System.out.println("Round " + PageRank.tempOutputCount + "------------" + line);
                
                //System.out.println("Im Map2");
                //System.out.println(line);
                
                if(line.length() > 0) {
                    String[] lineSplit = line.split("\\t");   //Split the input after the tab
                    String keyText = lineSplit[0];
                    String valueText = lineSplit[1];
                    String[] valueSplit = valueText.trim().split("-----");
                    
                    if(valueSplit.length > 1) {
                        if(valueSplit[0].equals("Links")) {
                            
                        	int nunodes = Integer.parseInt((valueSplit[1].split("#####*****"))[0]);
                        	
                        	//System.out.println( keyText + nunodes);
                        	
						context.write(new Text(keyText.trim()), new Text((1/(double) nunodes) + ""));
                        } else {
                            double pageRank = Double.parseDouble(valueSplit[0].trim());
                            String[] links = valueSplit[1].split("#####*****");
                            int numberOfLinks = Integer.parseInt(links[0].trim());
                            if(numberOfLinks > 0) {
                                double pageRankPercent = (double) pageRank / numberOfLinks;
                                for(int linkCount = 1; linkCount < links.length; linkCount++) {
                                	
                                    context.write(new Text(links[linkCount]), new Text(pageRankPercent + ""));
                                }
                            }
                        }
                        context.write(new Text(lineSplit[0]), new Text(lineSplit[1]));
                    }
                }
            } catch (Exception E) {
                E.printStackTrace();
            }
        }
		
	}
	
	
    public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> AllLinks, Context context) throws IOException, InterruptedException {
            double valuepageRank = 0;
			
			StringBuffer linksBuffer = new StringBuffer("");
			    
			for (Text link : AllLinks) {
				
				if(link.toString().length() > 0) {
					if(link.toString().contains("-----")) {
						// Splitting the link with delimiter	 
						String links[] = link.toString().split("-----");
						//overallPageRank
						linksBuffer.append(links[1]);
					}
					else {
						valuepageRank = valuepageRank + Double.parseDouble(link.toString());
					} 
				}
			} 
			valuepageRank = valuepageRank + (1 - class1.dampingFactor);
			String value = valuepageRank + "-----" + linksBuffer.toString();
			//System.out.println(value);
			context.write(key, new Text(value));          
        }
    }

}

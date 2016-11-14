package hadoop16.mapreduce;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.jar.JarFile;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.common.Pair;

import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.fs.Path;
import util.*;
import util.StringIntegerList.StringInteger;


public class LemmaIndexToTextMapred {
	public static class LemmaToTextMapper extends Mapper<Text, Text, Text, Text> {
private HashMap<String,String[]> professions=new HashMap<String,String[]>();
@Override
protected void setup(Mapper<Text, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
	// TODO Auto-generated method stub
	super.setup(context);
	
	loadProfessions();
}
		@Override
		public void map(Text key, Text value, Context context) throws IOException,
				InterruptedException {
			// TODO: You should implement inverted index mapper here
			
			String articleTitle=key.toString();
			String indices=value.toString();
			String[] prof=professions.get(articleTitle);
			if(prof==null)
				return;
			StringIntegerList indincesList=new StringIntegerList();
			indincesList.readFromString(indices);			
			List<StringInteger> outputList=indincesList.getIndices();
			Text outValue=new Text();
			Text outKey=new Text();
			String strValue="";
		
			for (StringInteger pair : outputList) {  
				for(int j=0;j<pair.getValue();j++)
					strValue+=pair.getString()+" ";		  
			} 
			outKey.set("/"+prof[0]+"/1");
			outValue.set(strValue);
			context.write(outKey,outValue);	
		}
		
private void loadProfessions() throws IOException {
		
			
			String PROFESSIONS_FILE = "professions.txt";
			
			ClassLoader cl = LemmaIndexToTextMapred.class.getClassLoader();
			
			String fileUrl = cl.getResource(PROFESSIONS_FILE).getFile();
			
			// Get jar path
			String jarUrl = fileUrl.substring(5, fileUrl.length() - PROFESSIONS_FILE.length() - 2);
			
			JarFile jf = new JarFile(new File(jarUrl));
			
			
			// Scan the people.txt file inside jar
			Scanner sc = new Scanner(jf.getInputStream(jf.getEntry(PROFESSIONS_FILE)),"UTF-8");
			String line=null;
			String name=null;
			String[] professionsArr=null;
			while(sc.hasNext())
			{
			
				line=sc.nextLine();
				if(line==null)
					continue;
				String[] splt=line.split(":",2);
				if(splt.length<2)
					continue;
				name=splt[0].trim();
				if("".equals(name))
					continue;
				if("".equals(splt[1].trim()))
						continue;
				professionsArr=splt[1].split(",");
				for (int i=0;i<professionsArr.length;i++)
					professionsArr[i]=professionsArr[i].trim();
								
				
				 professions.put(name,professionsArr);
				 
			
			}
			
			jf.close();
			sc.close();
		

		}
	}

	public static void main(String[] args) throws Exception {
		// TODO: you should implement the Job Configuration and Job call
		// here
	
		Configuration conf=new Configuration();
		GenericOptionsParser gop=new GenericOptionsParser(conf, args);
		String[] otherArgs=gop.getRemainingArgs();
		
		
		
			conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "	"); 
			Job job=Job.getInstance(conf, "Lemma to Text");
			job.setJarByClass(LemmaIndexToTextMapred.class);
			job.setMapperClass(LemmaToTextMapper.class);
//			job.setReducerClass(InvertedIndexReducer.class);
			job.setInputFormatClass(KeyValueTextInputFormat.class);		
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            
            FileInputFormat.addInputPath(job, new Path(args[1]));
             SequenceFileOutputFormat.setOutputPath(job, new Path(args[2]));
             job.waitForCompletion(true);
		
	
	}
	
}

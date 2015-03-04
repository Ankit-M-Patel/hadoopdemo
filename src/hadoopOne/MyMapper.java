package hadoopOne;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

	
		
		private Text word = new Text();
		private Text res = new Text();
		
		
		public void map(LongWritable key, Text value, Context context , Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {				
				word.set(tokenizer.nextToken());
				try {
					FileSplit fileSplit = (FileSplit)reporter.getInputSplit();									
					
					StringBuilder fileName = new StringBuilder(fileSplit.getPath().getName());
					fileName.append("@");
					
					fileName.append(key);
					res.set(fileName.toString());		
					
					
					context.write(word,res);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	


package hadoopOne;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

	
		
		private Text word = new Text();
		private Text res = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
			System.out.println("**************In Mapper ***********");
			/*
			String line = value.toString();
			Matcher m = p.matcher(value.toString());
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {				
				word.set(tokenizer.nextToken());
				try {
					FileSplit fileSplit = ((FileSplit) context.getInputSplit());									
					
					StringBuilder fileName = new StringBuilder(fileSplit.getPath().getName());
					fileName.append("@");
					
					fileName.append(key.toString());					
					res.set(fileName.toString());		
					
				
					context.write(word,new Text(res));
					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			*/
			Pattern p = Pattern.compile("\\w+");
			Matcher m = p.matcher(value.toString());

			// Get the name of the file from the inputsplit in the context
			String fileName = ((FileSplit) context.getInputSplit()).getPath()
					.getName();

			// build the values and write <k,v> pairs through the context
			StringBuilder valueBuilder = new StringBuilder();
			while (m.find()) {
				String matchedKey = m.group().toLowerCase();
				// remove special characters and digits
				if (!Character.isLetter(matchedKey.charAt(0))
						|| Character.isDigit(matchedKey.charAt(0))) {
					continue;
				}
				valueBuilder.append(fileName);
				valueBuilder.append("@");
				valueBuilder.append(key.get());
				// emit the partial <k,v>
				context.write(new Text(matchedKey),
						new Text(valueBuilder.toString()));
				valueBuilder.setLength(0);
		}
		}
	}
	
	


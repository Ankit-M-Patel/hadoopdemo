package hadoopOne;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MyDriver {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		System.out.println("Started Job");
		Job job = new Job(conf, "myJob");
		job.setJarByClass(MyDriver.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("input/Homes.txt"));
		FileOutputFormat.setOutputPath(job, new Path("demoOut"));
		
		long startTime = new Date().getTime();
		boolean status  = job.waitForCompletion(true);
		long end  = new Date().getTime();
		
		System.out.println("This job took "+ (end - startTime) + "milliseconds to complete!");
		
		
		System.exit(status ? 0 :1);
		
		
		
		
	}

}

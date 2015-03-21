package hadoopOne;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MyReducer extends Reducer<Text, Text, Text, Text>{


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException {

		try {
			StringBuilder valueBuilder = new StringBuilder();

			for (Text val : values) {


				valueBuilder.append(val);
				valueBuilder.append(",");
			}

			context.write(key, new Text(valueBuilder.substring(0, valueBuilder.length() - 1)));
			valueBuilder.setLength(0);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}





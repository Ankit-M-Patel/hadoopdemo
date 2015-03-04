package hadoopOne;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MyReducer extends Reducer<Text, Text, Text, Text>{


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException {

		Iterator<Text> iterator = values.iterator();
		Text res = new Text();
		StringBuilder vals = new StringBuilder();

		while (iterator.hasNext()) {
			vals.append(iterator.next());
			vals.append(",");
		}
		try {
			res.set(vals.toString());
			context.write(key, res);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}





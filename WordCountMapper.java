import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*; 
import java.util.StringTokenizer;


public class WordCountMapper extends Mapper <LongWritable,Text,Text,IntWritable> 
{
	private static final IntWritable countvalue = new IntWritable(1);
	private Text wordData = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws 
	IOException, InterruptedException
	{
		String lineData = value.toString();
		StringTokenizer wordsData = new StringTokenizer(lineData);
		while(wordsData.hasMoreTokens())
		{
			wordData.set(wordsData.nextToken());
			context.write(wordData,countvalue);
		}
	}

}

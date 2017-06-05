import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Lemma {
	static Map<String,String[]> lemmamap=new HashMap<String,String[]>();
	
	public static class MapperClass extends Mapper<LongWritable,Text,Text,Text> {
		
		private Text word = new Text();
		private Text location;
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			if(value.toString().contains(">"))
				location=new Text(value.toString().split(">")[0].concat(">,"));
			String[] tokens = value.toString().replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+");
			if (tokens.length > 1) {
				for (int i = 0; i < tokens.length - 1; i++) {
					tokens[i] = tokens[i].replaceAll("j", "i").replaceAll("v", "u").replaceAll("[^A-Za-z0-9\\s]", "");

					word.set(tokens[i]);
					//map.clear();
					context.write(word, location);
				}
			}
		}
		
	}

	public static class CombinerClass extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String temp = "";
			for (Text value : values) {
				temp += value.toString() + " ";
			}
			context.write(key, new Text(temp));
		}
	
	}
	
	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String temp = "";
			for (Text value : values) {
				temp += value.toString() + " ";
			}
			if (lemmamap.containsKey(key.toString())) {
				String[] list=null;
				list = lemmamap.get(key.toString());
				for (int i = 1; i < list.length; i++) {
					context.write(new Text(list[i]), new Text(temp));
				}
			} else {
				context.write(key, new Text(temp));
			}
		}
	   
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Lemma");
		job.setJarByClass(Lemma.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		try{
            Path pt=new Path("/home/hadoop/new_lemmatizer.csv");
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line=br.readLine();
            while (line != null){
            		String[] linetokens=line.split(",");
            		String lemma = linetokens[0];
            		if(linetokens.length>0){
            			for(int i=0;i<linetokens.length;i++ ){
            				lemmamap.put(lemma,linetokens);
            			}
            		}     
            }
            line=br.readLine();
    }catch(Exception e){
    }
		
		
		
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
		
		
	}
}
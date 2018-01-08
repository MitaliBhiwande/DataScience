import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Lemma3 {
	static Map<String,ArrayList<String>> lemmamap=new HashMap<String,ArrayList<String>>();
	
	public static class MapperClass extends Mapper<LongWritable,Text,Text,Text> {
		
		private Text first = new Text();
		private Text second = new Text();
		private Text third=new Text();
		private Text location=new Text();
		
		
		@Override
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {

			if(value.toString().contains(">"))
			{

				location.set(value.toString().split(">")[0].concat(">,"));

				String[] tokens = value.toString().split(">")[1].replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+");
				if (tokens.length > 1) {
					for (int i = 0; i < tokens.length - 3; i++) {
						first.set(tokens[i].replaceAll("j", "i").replaceAll("v","u"));
						ArrayList<String> firstlist=new ArrayList<String>();
						if(lemmamap.containsKey(first.toString()))
							firstlist.addAll(lemmamap.get(first.toString()));
						else
							firstlist.add(first.toString());

						second.set(tokens[i+1].replaceAll("j", "i").replaceAll("v","u"));
						ArrayList<String> secondlist=new ArrayList<String>();
						if(lemmamap.containsKey(second.toString()))
							secondlist.addAll(lemmamap.get(second.toString()));
						else
							secondlist.add(second.toString());

						third.set(tokens[i+2].replaceAll("j", "i").replaceAll("v","u"));
						ArrayList<String> thirdlist=new ArrayList<String>();
						if(lemmamap.containsKey(third.toString()))
							thirdlist.addAll(lemmamap.get(third.toString()));
						else
							thirdlist.add(third.toString());
						
						for(String k: firstlist)
							for(String l: secondlist)
								for(String m: thirdlist)
								context.write(new Text(k+" "+l+" "+m), location);
					}
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
				context.write(key, new Text(temp));
			
		}
	   
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Lemma3");
		job.setJarByClass(Lemma3.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setCombinerClass(CombinerClass.class);
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
            		ArrayList<String> list=new ArrayList<String>();
            		for(int i=1;i<linetokens.length;i++ ){
            				list.add(linetokens[i]);
            			}
            			lemmamap.put(lemma, list);
            			line=br.readLine();
            }
    }catch(Exception e){
    }
		
		
		
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
		
		
	}
}
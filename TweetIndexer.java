package org.myorg;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.*;
import org.apache.hadoop.mapred.JobPriority;

import twitter4j.JSONException;
import twitter4j.JSONObject;

/**
 * MapReduce Project
 * FUNCTION: Index the usernames and messages from a large collection of tweets from Twitter.
 * INPUT: JSON Objects representing Tweets.
 * OUTPUT: A list of words and metadata about their occurances in the tweets.
 */
public class TweetIndexer extends Configured implements Tool {
	// TODO: This class is no longer used. Figure out how to re-impliment it in place of
	// handling raw JSON strings. Also move this to its own .java file
	static public class DocPosting implements Writable, WritableComparable<DocPosting> {
		private Text docId;
		private ArrayWritable positions;
		private IntWritable docType; //0 = original, 1 = retweet, 2 = reply
		private IntWritable specialField; //0 = not special, 1 = word is in name, 2 = word is in username, 3 = word is in both

		public DocPosting() {
			docId = new Text();
			positions = new ArrayWritable(IntWritable.class);
			docType = new IntWritable();
			specialField = new IntWritable();
		}

		public void set(String docId, ArrayList<IntWritable> positions, int docType, int specialField) {
			this.docId.set(docId);
			this.positions.set(positions.toArray(new IntWritable[positions.size()]));
			this.docType.set(docType);
			this.specialField.set(specialField);
		}

		//Merges another posting with this posting
		public void merge(DocPosting otherPost) {
			ArrayList<IntWritable> newPositions = new ArrayList<IntWritable>();
			newPositions.addAll( this.getPositions() );
			newPositions.addAll( otherPost.getPositions() );

			this.positions.set(newPositions.toArray(new IntWritable[newPositions.size()]));

			//TODO: Handle special fields!
		}

		@Override
		public int compareTo(DocPosting post) {
			return this.docId.compareTo(post.getDocId());
		}

		public void write(DataOutput out) throws IOException {
			docId.write(out);
			positions.write(out);
			docType.write(out);
			specialField.write(out);
		}

		public void readFields(DataInput in) throws IOException {
			docId.readFields(in);
			positions.readFields(in);
			docType.readFields(in);
			specialField.readFields(in);
		}

		public DocPosting read(DataInput in) throws IOException {
			DocPosting p = new DocPosting();
			p.readFields(in);
			return p;
		}


		public int hashCode() {
			return docId.hashCode();
		}

		public String toJSONString() {
			StringBuilder sb = new StringBuilder();
			ArrayList<IntWritable> intWs = this.getPositions();	

			sb.append("{");
			sb.append("\"doc_id\":\"" + docId.toString() + "\",");
			sb.append("\"doc_type\":" + Integer.toString(docType.get()) + ",");
			sb.append("\"freq\":" + Integer.toString(intWs.size()) + ",");
			sb.append("\"positions\":[");

			for(int i = 0; i < intWs.size(); i++){
				sb.append(Integer.toString(((IntWritable) positions.get()[i]).get()));

				// If not last append a comma
				if(i < intWs.size() - 1){
					sb.append(",");
				}
			}

			sb.append("]");
			sb.append("}");
			return sb.toString();
		}

		public Text getDocId()    { return docId; }
		public ArrayList<IntWritable> getPositions() { 
			ArrayList<IntWritable> ret = new ArrayList<IntWritable>();

			for(Writable writable : positions.get()) {
				IntWritable intW = (IntWritable) writable;
				ret.add(intW);
			} 
			return ret;
		}
	}

	
	/**
	 * First Mapping.
	 * FUNCTION: Parse tweets for significant words to be indexed.
	 * INPUT: Username/Words+document ID paired with the word position (-1 == username position)
	 * OUTPUT: Username/Words+document ID paired with the word position 
	 */
	public static class TweetMessageParse_Phase1_Map extends Mapper<Object, Text, Text, IntWritable> {
		private String fileName = new String();

		// Get the file name (if needed...)
		protected void setup(Context context) throws IOException, InterruptedException {
			fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String screenName = null;
			String text = null;
			String docId = null;	
			int docType = 0;

			try {
				JSONObject tweet = new JSONObject(value.toString());
				text = tweet.getString("text");
				docId = tweet.getString("id_str");
				screenName = tweet.getJSONObject("user").getString("screen_name");

				if (tweet.has("in_reply_to_status_id_str") && tweet.getString("in_reply_to_status_id_str") != null) {
					docType = 2; //is reply
				}
				
				else if (tweet.has("retweeted_status")) {
					docType = 1; //is retweet
				}
				
				else {
					docType = 0; //is original post
				}	
			}
			
			catch(JSONException e) {
				// Give up on this entry is exception occurs.
				e.printStackTrace();
				return;
			}

			// Collect screenname output
			if(screenName != null) {
				Text nameAndId = new Text();
				nameAndId.set("@" + screenName + " " + docId + " " + Integer.toString(docType));
				context.write(nameAndId, new IntWritable(-1)); // Use -1 for the screen name position
			}

			// Parse through the message
			if(text != null) {
				ArrayList<String> toks_pass1 = new ArrayList<String>();
				ArrayList<String> toks_pass2 = new ArrayList<String>();
				
				// Parse list of tokens from message field, deliminate based on puntuation and spaces.
				toks_pass1.addAll(Arrays.asList(text.split("[^\\w@#]*\\s+[^\\w@#]*|\"")));

				// First Filter Pass: Remove all tokens that are not words, not URL links, not usernames, or not hashtags
				for(String str : toks_pass1) {
					if(str.startsWith("http") || str.startsWith("@") || str.startsWith("#") && str.length() > 2){
						toks_pass2.add(str);
					}

					else if(str.matches("[a-zA-Z].+")){				
						while(!str.matches("[a-zA-Z].*\\w") && str.length() > 2) {
							str = str.substring(0,str.length() - 1);
						}

						toks_pass2.add(str);
					}
				}

				// Second Filter Pass: Remove stop words
				for(int i = 0; i < toks_pass2.size(); i++) {
					String word = toks_pass2.get(i).toLowerCase();

					// Skip stop words
					// TODO: Move these to an external list file if time permits
					// TODO: Use a more convenient data structure?
					if(!word.equals("a") && !word.equals("an")	&& !word.equals("and") 
							&& !word.equals("are") && !word.equals("as") && !word.equals("at") 
							&& !word.equals("be") && !word.equals("by") && !word.equals("for")  
							&& !word.equals("from")	&& !word.equals("has")	&& !word.equals("in") 
							&& !word.equals("is") && !word.equals("it") && !word.equals("its") 
							&& !word.equals("of") && !word.equals("on")	&& !word.equals("that") 
							&& !word.equals("the") && !word.equals("to") && !word.equals("too") 
							&& !word.equals("was") && !word.equals("were") && !word.equals("will") 
							&& !word.equals("with")	&& !word.equals("i")&& !word.equals("")
							&& !word.equals(" ") && !word.equals("rt")) {

						Text wordAndId = new Text(); 
						// Separates the two strings with space. To be parsed later in reduce
						wordAndId.set(word + " " + docId + " " + Integer.toString(docType));

						//Collect output
						context.write(wordAndId,new IntWritable(i));
					}
				}
			}
		}
	}
	
	
	/**
	 * First Reduce.
	 * FUNCTION: Aggregate the incoming data into a list of words paired with metadata objects about the words
	 * INPUT: Username/Words+document ID paired with the word position (-1 == username position)
	 * OUTPUT: Each word found in a tweet paired with JSON formated metadata about the word's position in the tweet.
	 */
	public static class TweetMessageParse_Phase1_Reduce extends Reducer<Text, IntWritable, Text, Text> {
		Text word = new Text();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			String keyToks[] = key.toString().split(" ");
			word.set(keyToks[0]);
			String docId = keyToks[1];
			String docType = keyToks[2];
			ArrayList<Integer> positions = new ArrayList<Integer>();	

			for(IntWritable value : values){
				positions.add(new Integer(value.get()));
			}

			// Sort position numbers
			Collections.sort(positions);

			// Create a JSON object containing metadata about the tweet and the current word being indexed
			StringBuilder sb = new StringBuilder();	
			sb.append("{");
			sb.append("\"id\":\"" + docId + "\",");
			sb.append("\"type\":" + docType + ",");
			sb.append("\"freq\":" + Integer.toString(positions.size()) + ",");
			sb.append("\"pos\":[");

			for(int i = 0; i < positions.size(); i++){
				sb.append(Integer.toString(positions.get(i)));

				//If not last append a comma
				if(i < positions.size() - 1){
					sb.append(",");
				}
			}

			sb.append("]");
			sb.append("}");

			// Output the word paired with the metadata about the word's occurance
			context.write(word, new Text(sb.toString()));
		}
	}

	
	/**
	 * Second Map.
	 * Acts as a pass through so a second round of Reducing can be performed.
	 */
	public static class MetadataAggregation_Phase2_Map extends Mapper<LongWritable, Text, Text, Text> {		Text word = new Text();
		Text doc = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String toks[] = value.toString().split("\\s",2);//Only splits based on the first space
			word.set(toks[0]);
			doc.set(toks[1]);
			context.write(word, doc);
		}
	} 


	/**
	 * Second Reduce.
	 * FUNCTION: Aggregates all similar word tokens into a list.
	 * OUTPUT: A list (blank key in key-pair) of JSON Objects to be fed into the index web-end.
	 */
	public static class MetadataAggregation_Phase2_Reduce extends Reducer<Text, Text, Text, Text> {
		Text output = new Text();
		Text blank = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Begin creating a JSON Object via string builder. This will be the output
			StringBuilder sb = new StringBuilder();
			sb.append("{\"word\":\"" + key.toString() + "\",");
			sb.append("\"docs\":");
			sb.append("[");

			// Iterate through all values for this word and add to a list that we can sort
			// TODO: Might need to create another Map/Reduce phase if this task is too resource hungry. Seems fine for now...
			Iterator<Text> it = values.iterator();
			ArrayList<String> sortedList = new ArrayList<String>();

			while(it.hasNext()){
				Text value = it.next();
				sortedList.add(value.toString());
			}
			
			// Sort List based on frequency.
			Collections.sort(sortedList, new Comparator<String>() {
				@Override
				public int compare(String docInfo1, String docInfo2){
					int freq1 = 0;
					int freq2 = 0;

					// convert string to JSON
					try{
						JSONObject doc1JSON = new JSONObject(docInfo1);
						JSONObject doc2JSON = new JSONObject(docInfo2);
						freq1 = doc1JSON.getInt("freq");
						freq2 = doc2JSON.getInt("freq");
					}
					catch(JSONException e){
						e.printStackTrace();
						System.exit(-1);
					}

					return freq2 - freq1;
				}
			});

			// Finally append the metadata list to the JSON Object
			for(int i = 0; i < sortedList.size(); i++){
				sb.append(sortedList.get(i));

				//If not last, append a comma
				if(i < sortedList.size() - 1){
					sb.append(",");
				}
			}

			sb.append("]");
			sb.append("}");
			output.set(sb.toString());

			// Output as just a value with no key. The web interface will handle loading the index properly
			context.write(blank, output);
		}	
	}


	/* * * * * *  MAIN - MapReduce Driver * * * * * */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int ret = ToolRunner.run(conf, new TweetIndexer(), args);

		// Clean Up temp files left behind
		FileSystem fs = FileSystem.get(new Configuration());
		//fs.delete(new Path(args[1] + "-tmp"), true);
		//fs.delete(new Path(args[1] + "-tmp2"), true);

		System.exit(ret);
	}

	public int run(String[] args) throws Exception {
		// Create First Job - Parse all tweets and extract individual metadata
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);

		// Set Job priority to Very High as server is usually flooded with duplicate jobs
		conf.set("mapred.job.priority", JobPriority.VERY_HIGH.toString());
		
		Job job1 = new Job(conf, "TweetIndexer_Phase1");
		job1.setJarByClass(TweetIndexer.class);

		// Output - Word+DocID -> 
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		// MapOutput - Word+DocID -> Int
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);


		job1.setMapperClass(TweetMessageParse_Phase1_Map.class);
		job1.setReducerClass(TweetMessageParse_Phase1_Reduce.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "-tmp"));

		// Halt for job1 to be completed. Job2 CANNOT start until job1 is completed.
		if(!job1.waitForCompletion(true)){
			return 1;
		}	

		// Create Second Job - Combine all metadata for the same word.
		Configuration conf2 = getConf();
		conf2.set("mapred.child.java.opts","-Xmx4096m");
		conf2.set("mapred.job.priority", JobPriority.VERY_HIGH.toString());
		Job job2 = new Job(conf2, "TweetIndexer_Phase2");
		job2.setJarByClass(TweetIndexer.class);

		// Output - Word -> JSON Object
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		// Output - (BLANK) -> JSON Object for Index
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setMapperClass(MetadataAggregation_Phase2_Map.class);
		job2.setReducerClass(MetadataAggregation_Phase2_Reduce.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job2, new Path(args[1] + "-tmp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		if(!job2.waitForCompletion(true)){
			return 1;
		}

		return job2.waitForCompletion(true) ? 0 : 1;
	}
}	

/**
 *  Copyright (c) 2016 Apurv Verma
 */
package com.apurv.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * This class was written for processing http://snap.stanford.edu/data/memetracker9.html
 */
public class UrlSentiment {

  public static class UrlSentimentMapper
  extends Mapper<LongWritable, Text, LongText, FloatWritable> {
    private final static Text pUrl = new Text(); //current p_url
    private final static LongWritable timestamp = new LongWritable();
    private final static Set<String> topUrls = new HashSet<>();
    private final DateTimeFormatter f = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    private final List<String> comments = new ArrayList<>();
    
    private final FloatWritable sentimentScore = new FloatWritable();
    private final LongText timestampUrl = new LongText();

    protected void setup(Context context) throws IOException {
      //sa.init();
      //NLP.init();
      URI[] localPaths = context.getCacheFiles();
      FileSystem fs = FileSystem.get(context.getConfiguration());
      InputStream in = fs.open(new Path(localPaths[0]));
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String line = null;
      while((line = br.readLine()) != null) {
        String[] arr = line.split("\t");
        topUrls.add(arr[0]);        
      }
    }

    private String extractUrl(String url) throws URISyntaxException {
      URI uri = new URI(url);
      String domain = uri.getHost();
      return domain.startsWith("www.") ? domain.substring(4) : domain;
    }

    private void parseLine(String line) throws URISyntaxException {
      String[] arr = line.split("\t");
      if(arr[0].equals("P")) {
        pUrl.set(extractUrl(arr[1]));
      } else if(arr[0].equals("T")) {
        DateTime dt = f.parseDateTime(arr[1]);
        timestamp.set(dt.getMillis()/1000); //convert to seconds since epoch
      } else if(arr[0].equals("Q")) {
        comments.add(arr[1]);
      }
    }

    @Override
    public void map(LongWritable key, Text val, Context context) 
        throws IOException, InterruptedException {
      String line = val.toString();
      line = line.trim();
      //System.out.println(line);
      try{
        if(line.length() > 0) {
          context.getCounter(Stats.TOTAL_LINES).increment(1);
          parseLine(line);
        } else {
          if(pUrl.toString().length() > 0 &&
                     timestamp.get() > -1 && 
                     comments.size() > 0 &&
                     topUrls.contains(pUrl.toString())) {
            //compute the average sentiment
            float sigmaSentiment = 0;
            for(String comment : comments) {
              sigmaSentiment += 0;
            }
            float avgSentiment = sigmaSentiment/comments.size();
            sentimentScore.set(avgSentiment);
            timestampUrl.set(timestamp.get(), pUrl.toString());
            context.write(timestampUrl, sentimentScore);
          }
          pUrl.clear();
          timestamp.set(-1);
          comments.clear();
        }
      } catch(URISyntaxException e) {
        //If there is an error in parsing the url
        context.getCounter(Stats.BAD_URL).increment(1);
        e.printStackTrace();
        return;
      } catch(IllegalStateException e) {
        context.getCounter(Stats.BAD_TIME).increment(1);
        //If there is an error in parsing the time
        e.printStackTrace();
        return;
      } catch(Exception e) {
        context.getCounter(Stats.BAD_MISC).increment(1);
        //If there is some other kind of error
        e.printStackTrace();
        return;
      }
    }
  }
  
 
  public static class UrlSentimentReducer
  extends Reducer<LongText, FloatWritable, LongText, FloatWritable> {
    
    private final static FloatWritable avgSentimentScore = new FloatWritable();
    
    @Override
    public void reduce(LongText timestampUrl, Iterable<FloatWritable> sentimentScores,
        Context context) throws IOException, InterruptedException {
      float avgSentiment = 0;
      float count = 0;
      for(FloatWritable sentimentScore: sentimentScores) {
        avgSentiment += sentimentScore.get();
        count += 1;
      }
      avgSentiment = avgSentiment/count;
      avgSentimentScore.set(avgSentiment);
      context.write(timestampUrl, avgSentimentScore);
    }
    
  }

  public static void main(String[] args) throws IOException,
  ClassNotFoundException, InterruptedException, URISyntaxException {
    args = new String[3];
    args[0] = "/home/dapurv5/Downloads/quotes_2008-08-small.txt.gz";
    args[1] = "/home/dapurv5/Desktop/hdfs-output/meme_tracker";
    args[2] = "file:///home/dapurv5/Downloads/meme_tracker/nodes-500.tsv";
    args[2] = "/home/dapurv5/Downloads/meme_tracker/nodes-500.tsv";

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "url_sentiment");
    job.setJarByClass(UrlSentiment.class);

    //Use this instead of distributed cache
    job.addCacheFile(new URI(args[2]));

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(UrlSentimentMapper.class);
    job.setReducerClass(UrlSentimentReducer.class);

    job.setOutputKeyClass(LongText.class);
    job.setOutputValueClass(FloatWritable.class);
    System.exit(job.waitForCompletion(true)?0:1);
  }

}

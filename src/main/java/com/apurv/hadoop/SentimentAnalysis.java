package com.apurv.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class SentimentAnalysis {
  
  private final Set<String> posWords = new HashSet<>();
  private final Set<String> negWords = new HashSet<>();
  
  public void init() throws IOException {
    ClassLoader classLoader = getClass().getClassLoader();
    InputStream posStream = classLoader.getResourceAsStream("positive-words.txt");
    InputStream negStream = classLoader.getResourceAsStream("negative-words.txt");
    BufferedReader br = new BufferedReader(new InputStreamReader(posStream));
    String line = null;
    while((line = br.readLine()) != null) {
      posWords.add(line);
    }
    br = new BufferedReader(new InputStreamReader(negStream));
    line = null;
    while((line = br.readLine()) != null) {
      negWords.add(line);
    }
  }
  
  public float getSentiment(String text) {
    float count = 0;
    float sentiment = 0;
    String[] words = text.split(" ");
    for(String word: words) {
      word = word.trim();
      if(posWords.contains(word)) {
        sentiment += 1.0;
        count += 1.0;
      }
      if(negWords.contains(word)) {
        sentiment -= 1.0;
        count += 1.0;
      }
    }
    if(count > 0)
      sentiment = sentiment/count;
    return sentiment;
  }

  public static void main(String[] args) throws IOException {
    SentimentAnalysis sa = new SentimentAnalysis();
    sa.init();
    System.out.println(sa.getSentiment("telstra like a number of other major australian companies is affected by changes to superannuation laws made by the previous government we must of course comply with the law"));
  }

}

package com.apurv.hadoop;

import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class NLP {
  public static Properties props = new Properties();
  public static StanfordCoreNLP pipeline;

  public static void init() {
    props.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment");
    props.setProperty("parse.model", "edu/stanford/nlp/models/srparser/englishSR.ser.gz");
    pipeline = new StanfordCoreNLP(props);
  }

  public static int findSentiment(String tweet) {

    int longest = 0;
    int mainSentiment = 0;
    if (tweet != null && tweet.length() > 0) {
      Annotation document = new Annotation(tweet);
      pipeline.annotate(document);
      List<CoreMap> sentences = document.get(SentencesAnnotation.class);
      for(CoreMap sentence: sentences) {
        Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
        int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
        String partText = sentence.toString();
        if (partText.length() > longest) {
          mainSentiment = sentiment;
          longest = partText.length();
        }
      }
    }
    return mainSentiment;
  }

  public static void main(String[] args) {
    NLP.init();
    System.out.println(NLP.findSentiment("good"));
  }
}
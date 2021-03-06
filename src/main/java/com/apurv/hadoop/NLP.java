//package com.apurv.hadoop;
//
//import java.util.Properties;
//
//import edu.stanford.nlp.ling.CoreAnnotations;
//import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
//import edu.stanford.nlp.pipeline.Annotation;
//import edu.stanford.nlp.pipeline.StanfordCoreNLP;
//import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
//import edu.stanford.nlp.trees.Tree;
//import edu.stanford.nlp.util.CoreMap;
//
////this works with version 3.4.1
//public class NLP {
//  public static Properties props = new Properties();
//  public static StanfordCoreNLP pipeline;
//
//  public static void init() {
//    props.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment");
//    props.setProperty("parse.model", "edu/stanford/nlp/models/srparser/englishSR.ser.gz");
//    pipeline = new StanfordCoreNLP(props);
//  }
//
//  public static float findSentiment(String tweet) {
//
//    float mainSentiment = 0;
//    if (tweet != null && tweet.length() > 0) {
//        int longest = 0;
//        Annotation annotation = pipeline.process(tweet);
//        for (CoreMap sentence : annotation
//                .get(CoreAnnotations.SentencesAnnotation.class)) {
//            Tree tree = sentence
//                    .get(SentimentCoreAnnotations.AnnotatedTree.class);
//            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
//            String partText = sentence.toString();
//            if (partText.length() > longest) {
//                mainSentiment = sentiment;
//                longest = partText.length();
//            }
//
//        }
//    }
//    return mainSentiment/4.0f;
//}
//
//  public static void main(String[] args) {
//    NLP.init();
//    System.out.println(NLP.findSentiment("It was a great movie. The story was awesome, I highly recommend it."));
//  }
//}
echo -e "\n\n---===Creating Jar File===---"
jar -cvf TweetIndexer.jar -C TweetIndexer_classes/ .
##rm ~/cs236project_classes/
hadoop fs -rm -R output
hadoop fs -rm -R output-tmp
hadoop fs -rm -R output-tmp2

echo -e "\n\n---===Running Hadoop===---"
export LIBJARS=~/twitter4j/twitter4j-core-4.0.3.jar
hadoop jar ~/TweetIndexer.jar org.myorg.TweetIndexer -libjars ~/twitter4j/twitter4j-core-4.0.3.jar data output

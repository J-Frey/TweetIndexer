echo -e "\n\n---===Compiling & Making Files===---"
export CLASSPATH=/etc/hadoop/conf:/usr/lib/hadoop/*:/usr/lib/hadoop-hdfs/*:/usr/lib/hadoop-yarn/.//*:/usr/lib/hadoop-0.20-mapreduce/*:~/twitter4j/*
rm -R TweetIndexer_classes
mkdir TweetIndexer_classes
javac -cp $CLASSPATH -d TweetIndexer_classes TweetIndexer.java
echo -e "\nDone!\n"

# mapreduce_text_2_seq
LemmaToSeq is using Java and Mahout libraries to convert Text to Sequence<br>
LemmaIndexToText is using Mapreduce and Mahout libraries to convert Text to Sequence<br><br>

<strong>run LemmaToSeq by this command: </strong><br>
bin/yarn jar path-to-the-jar-file.jar hadoop16.converter.LemaToSeq -Dmapreduce.job.queuename=hadoop16 path-to-the-lemma-index path-to-the-output path-to-the-professions.txt
<br><br>
<strong>run LemmaIndexToText by this command: </strong><br>
bin/yarn jar path-to-the-jar-file.jar hadoop16.mapreduce.LemmaIndexToTextMapred -Dmapreduce.job.queuename=hadoop16 path-to-the-lemma-index path-to-the-output


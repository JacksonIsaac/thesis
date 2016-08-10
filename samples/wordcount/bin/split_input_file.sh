#/bin/bash


cp=$TWISTER_HOME/bin:.

for i in ${TWISTER_HOME}/lib/*.jar;
  do cp=$i:${cp}
done

for i in ${TWISTER_HOME}/apps/*.jar;
  do cp=$i:${cp}
done

java -Xmx512m -Xms512m -XX:SurvivorRatio=10 -classpath $cp cgl.imr.samples.wordcount.WCFileSplit $1 $2 $3 $4


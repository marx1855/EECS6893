mvn clean package
hdfs dfs -rm -r output/bitcoin
hadoop jar target/bitcoin-0.0.1-SNAPSHOT.jar EECS6893.bitcoin.BitCoinRunner 
hdfs dfs -cat output/bitcoin/*

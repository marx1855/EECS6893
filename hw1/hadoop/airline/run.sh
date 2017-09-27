mvn clean package
hdfs dfs -rm -r output/airline/*
hadoop jar target/airline-0.0.1-SNAPSHOT.jar EECS6893.airline.AirlineAnalysisRunner 
hdfs dfs -cat output/airline/day_of_week/*

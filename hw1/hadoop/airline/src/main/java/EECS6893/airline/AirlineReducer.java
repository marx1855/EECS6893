package EECS6893.airline;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AirlineReducer extends Reducer <IntWritable, Text, Text, Text>{
	HashMap<Integer, String> res;
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		if (res == null) res = new HashMap<Integer, String>();
		int sum = 0;
		int delayedNum = 0;
		int earlyNum = 0;
		int depDelayedNum = 0;
		int depEarlyNum = 0;
		int cancelledNum = 0;
		for (Text t: values) {
			String[] info = t.toString().split(",");
			/**
			 *  0:	Year
			 *  1:	Month
			 *	2:	DOM
			 *	3:	DOW
			 *	4:	DepT
			 *	5:	CRSDepT
			 *	6:	ArrT
			 *	7:	CRSArrT
			 *	8:	UniqueCarrier
			 *	9:	FlightNum
			 *	10:	TailNum
			 *	11:	ActualElapsedT
			 *	12:	CRSElapsedT
			 *	13:	ArrT
			 *	14:	ArrDelay
			 *	15:	DepDelay
			 *	16:	Origin
			 *	17:	Dest
			 *	18:	Distance
			 *	19:	TaxiIn
			 *	20:	TaxiOut
			 *	21:	Cancelled
			 *	22:	CancellationCode
			 *	23:	Diverted
			 *	24:	CarrierDelay
			 *	25:	WeatherDelay
			 *	26:	NASDelay
			 *	27:	SecurityDelay
			 *	28:	LateAircraftDelay
			 */
				
			if (!info[14].equals("NA")) {
				int delay = Integer.parseInt(info[14]);
				if (delay > 5) delayedNum++;
				else if (delay < -5) earlyNum++;
			}
			
			if (!info[15].equals("NA")) {
				int delay = Integer.parseInt(info[15]);
				if (delay > 5) depDelayedNum++;
				else if (delay < -5) depEarlyNum++;
			}
			
			if (info[21].equals("1")) cancelledNum++;
			
			
			sum++;
			
		}
		int day = key.get();
		
		String s = String.format("%-7d\t%-7d\t%2.1f%%\t%-7d\t%2.1f%%\t%-7d\t%2.1f%%\t%-7d\t%2.1f%%", 
								  sum,
								  delayedNum, (double)delayedNum/sum * 100,
								  depDelayedNum, (double)depDelayedNum/sum * 100,
								  earlyNum, (double)earlyNum/sum * 100,
								  depEarlyNum, (double)depEarlyNum/sum * 100);
		//context.write(new Text(String.valueOf(day)), new Text(s));
		res.put(day, s);
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Text headerKey = new Text("DoW");
		Text headerValue = new Text(String.format("%-7s\t%-7s\tpct\t%-7s\tpct\t%-7s\tpct\t%-7s\tpct",
									"Sum", "ArrDly", "DepDly", "ArrEly", "DepEly"));
		context.write(headerKey, headerValue);
		
		/*for (int i: res.keySet()) {
			System.out.println("day = " + String.valueOf(i));
		}*/
		
		for (int i = 1; i <= 7; i++) {
			context.write(new Text(String.valueOf(i)), new Text(res.get(i)));
		}
	}
}

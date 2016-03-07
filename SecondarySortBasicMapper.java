package com.amex.acxiom;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortBasicMapper extends
  	Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
//		Configuration conf = context.getConfiguration();
//        int groupByCol = Integer.parseInt(conf.get("GroupByCol").trim());
//        int sortByCol = Integer.parseInt(conf.get("SortByCol").trim());

		if (value.toString().length() > 0) {
			String arrEmpAttributes[] = value.toString().split("\\t");

			context.write(
					new CompositeKeyWritable(
							arrEmpAttributes[6].toString(),
							(arrEmpAttributes[3].toString() + "\t"
									+ arrEmpAttributes[2].toString() + "\t" + arrEmpAttributes[0]
									.toString())), NullWritable.get());
		}

	}
}
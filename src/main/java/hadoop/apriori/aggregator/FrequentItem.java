package hadoop.apriori.aggregator;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class FrequentItem extends BaseOperation<FrequentItem.Context> implements Aggregator<FrequentItem.Context> {
	class Context {
		int size;
	}

	private int support;
	private Path path;
	private JobConf conf;
	public FrequentItem(int support, Fields fields) {
		super(1,fields);
		this.support = support;
	}

	public void aggregate(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
		Context context = aggregatorCall.getContext();
		context.size++;
	}

	public void complete(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
		Context context = aggregatorCall.getContext();
		if(context.size > this.support){
			Tuple tuple = new Tuple();
			tuple.add(context.size);
			aggregatorCall.getOutputCollector().add(tuple);
		}
	}

	public void start(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
		Context context = new Context();
		aggregatorCall.setContext(context);

	}
}

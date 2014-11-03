package hadoop.apriori.aggregator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/*
 * ProductId implements Aggregator, provide session  products("," separated).
 * */
public class ProductId extends BaseOperation<ProductId.Context> implements Aggregator<ProductId.Context> {

	class Context {
		Map<Integer, Boolean> productMap = new LinkedHashMap<Integer, Boolean>();
	}

	public ProductId(Fields fields) {
		super(1, fields);
	}

	/**
	 * Start of the aggregator, add Context to the aggregatorCall
	 * 
	 * @param flowProcess
	 * @param aggregatorCall
	 */
	public void start(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
		Context context = new Context();
		aggregatorCall.setContext(context);
	}

	/**
	 * Aggregate all the product in the product map for the session, called for
	 * every tuple of this group.
	 * 
	 * @param flowProcess
	 * @param aggregatorCall
	 */
	public void aggregate(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
		TupleEntry arguments = aggregatorCall.getArguments();
		Context context = aggregatorCall.getContext();
		int product = arguments.getInteger("product");
		if (context.productMap.get(product) == null) {
			context.productMap.put(product, true);
		}
	}

	/**
	 * Returns products for the group and adds tuple. Called on complete.
	 * 
	 * @param flowProcess
	 * @param aggregatorCall
	 */
	public void complete(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
		Context context = aggregatorCall.getContext();
		Tuple result = new Tuple();
		if (context.productMap.size() < 2) {
			return;		//Returns if in a session only 1 product is visited.
		}
		Iterator<Integer> keys = context.productMap.keySet().iterator();
		StringBuilder productIds = new StringBuilder();
		while (keys.hasNext()) {
			int key = keys.next();
			productIds.append(key);
			if (keys.hasNext()) {
				productIds.append("|");
			}
		}
		result.add(productIds.toString());
		aggregatorCall.getOutputCollector().add(result);
	}
}

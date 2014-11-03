package hadoop.apriori.functions;

import cascading.flow.AssemblyPlanner.Context;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class CreateL1 extends BaseOperation<Context> implements Function<Context> {

	public CreateL1(Fields fields) {
		super(1, fields);

	}

	public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {
		// functionCall.
		TupleEntry arguments = functionCall.getArguments();

		String products = arguments.getString("products");
		String[] product = products.split("\\|");
		for (int i = 0; i < product.length; i++) {
			Tuple tuple = new Tuple();
			tuple.add(Integer.parseInt(product[i]));
			functionCall.getOutputCollector().add(tuple);
		}
	}
}

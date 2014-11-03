package hadoop.apriori.functions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import cascading.flow.AssemblyPlanner.Context;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class CreateL2 extends BaseOperation<Context> implements Function<Context> {
	private HashMap<String, String> map = new HashMap<String, String>();
	public CreateL2(Fields fields, JobConf conf) {
		super(1,fields);
		try {
			URI[] uri = DistributedCache.getCacheFiles(conf);
			FileSystem fs = new Path(".").getFileSystem(conf);
			if (fs.exists(new Path(uri[0]))) {
				FSDataInputStream inStream = fs.open(new Path(uri[0]));
				BufferedReader cacheReader = new BufferedReader(new InputStreamReader(inStream));
				String line = null;
				while ((line = cacheReader.readLine()) != null) {
			        String[] value = line.split("\t");
			        map.put(value[0], value[1]);
			    }
			}
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}
	 

	public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {
		TupleEntry arguments = functionCall.getArguments();
		String products = arguments.getString("products");
		String[] product = products.split("\\|");
		for (int i = 0; i < product.length-1; i++) {
			if(!this.map.containsKey(product[i])){
				continue;
			}
			for(int j=i+1; j<product.length; j++){
				if(this.map.containsKey(product[j])){
					Tuple tuple = new Tuple();
					tuple.add(Integer.parseInt(product[i])+","+Integer.parseInt(product[j]));
					functionCall.getOutputCollector().add(tuple);
				}
			}
		}
	}

}

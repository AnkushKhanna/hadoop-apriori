package hadoop.apriori.flow;

import hadoop.apriori.aggregator.FrequentItem;
import hadoop.apriori.functions.CreateL1;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class AprioriFlowL1 {
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Flow createFlow(String inputFolder, String outputFolder) {
		// Input Tap
		Fields sourceField = new Fields("session", "products");
		Scheme sourceScheme = new TextDelimited(sourceField);
		Tap source = new Hfs(sourceScheme, inputFolder);

		// Output Tap
		Fields sinkField = new Fields("items", "count");
		Scheme sinkScheme = new TextDelimited(sinkField);
		Hfs sink = new Hfs(sinkScheme, outputFolder, SinkMode.REPLACE);		
		
		// Create Pipe
		Pipe assembly = new Pipe("apriori");
		//Each
		assembly = new Each(assembly, new CreateL1(new Fields("items")));

		assembly = new GroupBy(assembly, new Fields("items"));
		
		assembly = new Every(assembly, new FrequentItem(250, new Fields("count")));
		
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, AprioriFlowL1.class);
		FlowDef flowDef = FlowDef.flowDef();
		flowDef.setName("apriori").addSource(assembly, source).addTailSink(assembly, sink);
	
		// Flow
		FlowConnector flowConnector = new HadoopFlowConnector(properties);
		Flow flow = flowConnector.connect(flowDef);
		return flow;
	}
}

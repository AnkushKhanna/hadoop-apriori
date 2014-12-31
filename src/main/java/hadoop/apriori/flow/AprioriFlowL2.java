package hadoop.apriori.flow;

import hadoop.apriori.aggregator.FrequentItem;
import hadoop.apriori.functions.CreateL2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

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

public class AprioriFlowL2 {
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Flow createFlow(String inputFolder,
			String intermediateFolder, String outputFolder) {
		// Input Tap
		Fields sourceField = new Fields("session", "products");
		Scheme sourceScheme = new TextDelimited(sourceField);
		Tap source = new Hfs(sourceScheme, inputFolder);
		JobConf conf = new JobConf(new Configuration());
		conf.setJarByClass(AprioriFlowL2.class);

		try {
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] file_status = fs.listStatus(new Path(
					intermediateFolder));
			for (FileStatus fileStatus : file_status) {
				String fileName = fileStatus.getPath().getName();
				if (!fileName.startsWith("_")) {
					DistributedCache.addCacheFile(new URI(intermediateFolder
							+ "/" + fileName), conf);
				}
			}

		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Output Tap
		Fields sinkField = new Fields("items", "count");
		Scheme sinkScheme = new TextDelimited(sinkField);
		Hfs sink = new Hfs(sinkScheme, outputFolder, SinkMode.REPLACE);
		// Create Pipe
		Pipe assembly = new Pipe("apriori");
		// Each
		assembly = new Each(assembly, new CreateL2(new Fields("items"), conf));

		assembly = new GroupBy(assembly, new Fields("items"));

		assembly = new Every(assembly, new FrequentItem(250,
				new Fields("count")));

		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, AprioriFlowL2.class);
		FlowDef flowDef = FlowDef.flowDef();
		flowDef.setName("apriori").addSource(assembly, source)
				.addTailSink(assembly, sink);

		// Flow
		FlowConnector flowConnector = new HadoopFlowConnector(properties);
		Flow flow = flowConnector.connect(flowDef);
		return flow;
	}
}

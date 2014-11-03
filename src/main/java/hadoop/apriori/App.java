package hadoop.apriori;


import hadoop.apriori.flow.AprioriFlowL1;
import hadoop.apriori.flow.AprioriFlowL2;
import cascading.flow.Flow;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	String inputFile= "results/part-00000";
    	String outputFolder = "apriori-result-L1";
    	Flow aprioriFlow = AprioriFlowL1.createFlow(inputFile, outputFolder);
    	aprioriFlow.complete();
    	outputFolder = "apriori-result-L2";
    	Flow aprioriFlow2 = AprioriFlowL2.createFlow(inputFile, outputFolder);
    	aprioriFlow2.complete();
    }
}

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
    	String outputFolderL1 = "apriori-result-L1";
    	Flow aprioriFlow = AprioriFlowL1.createFlow(inputFile, outputFolderL1);
    	aprioriFlow.complete();
    	String outputFolderL2 = "apriori-result-L2";
    	Flow aprioriFlow2 = AprioriFlowL2.createFlow(inputFile,outputFolderL1, outputFolderL2);
    	aprioriFlow2.complete();
    }
}

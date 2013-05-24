

import java.util.Scanner;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class GoldbachTopology {
	
	public static void main(String[] args) throws Exception {
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("spout", new EvenNumbersSpout(), 1);
        
        builder.setBolt("find_prime_sum", new FindPrimeSumBolt(), 8)
                 .shuffleGrouping("spout");
        
        builder.setBolt("outputter", new OutputterBolt(), 3)
        		 .shuffleGrouping("find_prime_sum");

        Config conf = new Config();
        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(9);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {        
            conf.setMaxTaskParallelism(12);

            LocalCluster cluster = new LocalCluster();
        
            Scanner scanner = new Scanner(System.in); 
        	System.out.println("Press enter to start the computation and press it again to stop.");
            scanner.nextLine();
            cluster.submitTopology("goldbach", conf, builder.createTopology());
            scanner.nextLine();

            cluster.shutdown();
        }
    }
}

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class OutputterBolt extends BaseRichBolt {

    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }
    
    @Override
    public void execute(Tuple tuple) {
        Integer candidate = tuple.getInteger(0);
        Integer num1 = tuple.getInteger(1);
        Integer num2 = tuple.getInteger(2);
        Integer numTests = tuple.getInteger(3);
        if (num1 == null || num2 == null)
        {
	        StringBuilder b = new StringBuilder();
        	b.append("FAIL: ")
        	 .append(candidate)
        	 .append(", ")
        	 .append(numTests)
        	 .append(" tests");
	        System.out.println(b.toString());
        }
        else if (candidate % 10000 == 0)
        {
	        StringBuilder b = new StringBuilder();
	        b.append(candidate)
	         .append(" = ")
	         .append(num1)
	         .append(" + ")
	         .append(num2)
	         .append(", ")
	         .append(numTests)
	         .append(" tests");
	        System.out.println(b.toString());
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}


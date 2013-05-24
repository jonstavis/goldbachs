import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


public class FindPrimeSumBolt extends BaseRichBolt {

	static final Integer CHUNK_SIZE = 1000;
	
    OutputCollector _collector;
    
    MongoClient _mongoClient;
    DB _db;
    DBCollection _collection;
    DBObject _ascSort;
    DBObject _descSort;
    
    Integer _numTests;
    
    // no need to re-query this for each number
    List<Integer> _lower = new ArrayList<Integer>();
    // use this for lower numbers that may be higher than the 1000th prime - this happens VERY infrequently 
    List<Integer> _emergencyLower;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) 
    {
        _collector = collector;
        try 
        {
	        _mongoClient = new MongoClient();
	        _db = _mongoClient.getDB("goldbach");
	        _collection = _db.getCollection("primes");
	        _ascSort = new BasicDBObject("prime", 1);
	        _descSort = new BasicDBObject("prime", -1);
        }
        catch (UnknownHostException uhe) { }
    }
    
    @Override
    public void execute(Tuple tuple) 
    {
    	if (_collection == null)
    		_collector.fail(tuple);
        Integer candidate = tuple.getInteger(0);
        _numTests = 0;
        
        _emergencyLower = new ArrayList<Integer>();
        
        if (_lower.size() < CHUNK_SIZE)
        {
	        DBCursor lowerCur = getBottomPrimesChunk(candidate, CHUNK_SIZE, 0);
	        while (lowerCur.hasNext())
	        	_lower.add((Integer) lowerCur.next().get("prime"));
        }
        DBCursor upper = getTopPrimesChunk(candidate, CHUNK_SIZE, 0);
        upper.next();
       
        if (checkMatch(candidate, 0, upper, 0, 0))
	        _collector.ack(tuple);
        else
        	_collector.fail(tuple);
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        declarer.declare(new Fields("candidate","num1","num2","num_tests"));
    }

    private DBCursor getBottomPrimesChunk(Integer candidate, Integer chunkSize, Integer offset)
    {
    	DBObject lessThanCandidate = new BasicDBObject("prime", new BasicDBObject("$lt", candidate));
    	return _collection.find(lessThanCandidate).sort(_ascSort).limit(chunkSize).skip(offset);
    }
    private DBCursor getTopPrimesChunk(Integer candidate, Integer chunkSize, Integer offset)
    {
    	DBObject lessThanCandidate = new BasicDBObject("prime", new BasicDBObject("$lt", candidate));
    	return _collection.find(lessThanCandidate).sort(_descSort).limit(chunkSize).skip(offset);
    }
    private boolean checkMatch(int candidate, 
							   int lowerIndex, 
							   DBCursor upper, 
							   Integer lowerOffset, 
							   Integer upperOffset)
    {
    	int prime1, prime2, test;
    	List<Integer> currentLower = _lower;
    	if (_emergencyLower != null && _emergencyLower.size() > 0)
    		currentLower = _emergencyLower;
    		
    	while (lowerIndex < currentLower.size() && upper.hasNext())
    	{
    		prime1 = (Integer) currentLower.get(lowerIndex);
    		prime2 = (Integer) upper.curr().get("prime");
    		test = prime1 + prime2;
    		_numTests++;
    		
    		if (test == candidate)
    		{
		        _collector.emit(new Values(candidate, prime1, prime2, _numTests));
		        return true;
    		}
    		else if (test < candidate)
    		{
    			lowerIndex++;
    		}
    		else if (test > candidate)
    		{
    			upper.next();
    		}
    	}
    	
    	if (lowerIndex == currentLower.size())
    	{
    		DBCursor cur = getBottomPrimesChunk(candidate, CHUNK_SIZE, lowerOffset += CHUNK_SIZE);
    		_emergencyLower.clear();
    		while (cur.hasNext())
    			_emergencyLower.add((Integer) cur.next().get("prime"));
    		return checkMatch(candidate, 
    						  0,
    						  upper,
    						  lowerOffset, 
    						  upperOffset);
    	}
    	else if (!upper.hasNext())
    		return checkMatch(candidate, 
    						  lowerIndex,
    						  getTopPrimesChunk(candidate, CHUNK_SIZE, upperOffset += CHUNK_SIZE),
    						  lowerOffset, 
    						  upperOffset);
    	
    	// this means there are no more primes to look through that are less than the candidate
    	// if conjecture is true this should never happen
        _collector.emit(new Values(candidate, null, null, _numTests));
		return false;
    }
}


import java.util.Arrays;

import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;


public class TrafficPredictionClient {

	private Cluster theCluster;
	private KeyspaceDefinition keyspaceDef;
	private Keyspace ksp;
	//private ColumnFamilyTemplate<String, String> template;
	private ColumnFamilyTemplate<String, Integer> template;
	
	public static void main(String[] args)
	{
		TrafficPredictionClient tpc = new TrafficPredictionClient();
		tpc.init();
		
		tpc.testInsert();
		tpc.testRead();
	}
	
	public void init()
	{
		theCluster = HFactory.getOrCreateCluster("test-cluster","localhost:9160");
		System.out.println("Created cluster object");
		
		keyspaceDef = theCluster.describeKeyspace("MyKeyspace");
		
		// If keyspace does not exist, the CFs don't exist either. => create them.
		if (keyspaceDef == null)
		{
		    createSchema();
		}
		
		ksp = HFactory.createKeyspace("MyKeyspace", theCluster);
		System.out.println("Created keyspace object");
		
		//template = new ThriftColumnFamilyTemplate<String, String>(ksp, "ColumnFamilyName", StringSerializer.get(), StringSerializer.get());
		template = new ThriftColumnFamilyTemplate<String, Integer>(ksp, "ColumnFamilyName", StringSerializer.get(), IntegerSerializer.get());
		
		
	}
	
	public void createSchema()
	{
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition("MyKeyspace", "ColumnFamilyName", ComparatorType.BYTESTYPE);
		System.out.println("Created column family definition");
		
		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition("MyKeyspace", ThriftKsDef.DEF_STRATEGY_CLASS, 1, Arrays.asList(cfDef));
		System.out.println("Created keyspace definition");
		
		//Add the schema to the cluster.
		//"true" as the second param means that Hector will block until all nodes see the change.
		theCluster.addKeyspace(newKeyspace, true);
		System.out.println("Added keyspace definition to cluster");
	}
	
	public void testInsert()
	{
//		// <String, String> correspond to key and Column name.
//		ColumnFamilyUpdater<String, String> updater = template.createUpdater("a key");
//		
//		updater.setString("domain", "www.datastax.com");
//		updater.setLong("time", System.currentTimeMillis());
//
//		try
//		{
//		    template.update(updater);
//		}
//		catch (HectorException e)
//		{
//		    // do something ...
//		}
		
		ColumnFamilyUpdater<String, Integer> updater = template.createUpdater("a key");
		updater.setDouble(0, 54.7);
		updater.setDouble(1, 23.5);
		updater.setDouble(2, 77.0);
		updater.setDouble(3, 45.0);
		
		try
		{
			template.update(updater);
		}
		catch (HectorException e)
		{
			
		}
	}
	
	public void testRead()
	{
//		try
//		{
//		    ColumnFamilyResult<String, String> res = template.queryColumns("a key");
//		    String value = res.getString("domain");
//		    System.out.println("Retrieved value: " + value);
//		    // value should be "www.datastax.com" as per our previous insertion.
//		}
//		catch (HectorException e) {
//		    // do something ...
//		}
		
		SliceQuery<String, Integer, Double> q = HFactory.createSliceQuery(ksp, StringSerializer.get(), IntegerSerializer.get(), DoubleSerializer.get());
		q.setColumnFamily("ColumnFamilyName");
		q.setKey("a key");
		q.setRange(1, 3, false, 4);
		QueryResult<ColumnSlice<Integer, Double>> resultSet = q.execute();
		ColumnSlice<Integer, Double> slice = resultSet.get();
		for (HColumn<Integer, Double> c : slice.getColumns())
		{
			System.out.println(c.getName() + " : " + c.getValue());
		}
	}
}

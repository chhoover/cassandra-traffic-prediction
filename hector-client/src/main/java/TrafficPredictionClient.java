import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Scanner;
import java.util.TreeMap;

import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
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
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;


public class TrafficPredictionClient 
{
	private Cluster theCluster;
	private KeyspaceDefinition keyspaceDef;
	private Keyspace ksp;
	private ColumnFamilyTemplate<String, Long> template;
	
	private SimpleDateFormat dateTemplate;
	private Calendar firstDateInDb;
	
	private static final String CLUSTER_NAME = "test-cluster";
	private static final String CLUSTER_LOCATION = "localhost:9160";
	private static final String KEYSPACE_NAME = "MyKeyspace";
	private static final String COLUMN_FAMILY_NAME = "ColumnFamilyName";
	
	private static final String TIMESTAMP_FORMAT = "MM'/'dd'/'yyyy' 'HH':'mm':'ss";
	
	public static void main(String[] args)
	{
		TrafficPredictionClient tpc = new TrafficPredictionClient();
		tpc.init();
		
		tpc.getPredictedSpeed(400014, "10/03/2012 16:43:00");
	}
	
	/**
	 * Connects to the Cassandra database and performs various initialization tasks.
	 */
	public void init()
	{
		theCluster = HFactory.getOrCreateCluster(CLUSTER_NAME,CLUSTER_LOCATION);
		System.out.println("Created cluster object");
		
		keyspaceDef = theCluster.describeKeyspace(KEYSPACE_NAME);
		
		// If keyspace does not exist, the CFs don't exist either. => create them.
		if (keyspaceDef == null)
		{
		    createSchema();
		}
		
		ksp = HFactory.createKeyspace(KEYSPACE_NAME, theCluster);
		System.out.println("Created keyspace object");

		template = new ThriftColumnFamilyTemplate<String, Long>(ksp, COLUMN_FAMILY_NAME, StringSerializer.get(), LongSerializer.get());
		
		dateTemplate = new SimpleDateFormat(TIMESTAMP_FORMAT);
		
		firstDateInDb = Calendar.getInstance();
		try
		{
			firstDateInDb.setTime(dateTemplate.parse("09/01/2012 00:00:00"));
		}
		catch (ParseException e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * Creates a keyspace and column family definition in the Cassandra database.
	 */
	public void createSchema()
	{
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(KEYSPACE_NAME, COLUMN_FAMILY_NAME, ComparatorType.BYTESTYPE);
		System.out.println("Created column family definition");
		
		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(KEYSPACE_NAME, ThriftKsDef.DEF_STRATEGY_CLASS, 1, Arrays.asList(cfDef));
		System.out.println("Created keyspace definition");
		
		//Add the schema to the cluster.
		//"true" as the second param means that Hector will block until all nodes see the change.
		theCluster.addKeyspace(newKeyspace, true);
		System.out.println("Added keyspace definition to cluster");
	}
	
	/**
	 * Reads a set of raw text data files from the CalTrans archive
	 * and inserts their contents into the Cassandra database.
	 * @param dataFolderPath Directory that contains the data files.
	 * @throws IOException
	 */
	public void loadTrafficData(String dataFolderPath) throws IOException
	{
		File dataFolder = new File(dataFolderPath);
		for (File f : dataFolder.listFiles())
		{
			System.out.println("Found file: " + f.getName());
			
			if (f.getName().endsWith(".txt"))
			{
				Scanner fileReader = new Scanner(f);
				System.out.println("Reading file: " + f.getName());
				
				int valuesInserted = 0;
				
				while (fileReader.hasNextLine())
				{
					try
					{
						String[] fields = fileReader.nextLine().split(",");
						
						if (fields[11].length() == 0)
							continue;
						
						String timestampStr = fields[0];
						Date date = dateTemplate.parse(timestampStr);
						long timestamp = date.getTime();
												
						int stationId = Integer.parseInt(fields[1]);
						double avgSpeed = Double.parseDouble(fields[11]);
						
						String key = constructKey(stationId, date);
						
						ColumnFamilyUpdater<String, Long> updater = template.createUpdater(key);
						updater.setDouble(timestamp, avgSpeed);
						
						try
						{
							template.update(updater);
							++valuesInserted;
							
							if (valuesInserted % 1000 == 0)
								System.out.println("Inserted " + valuesInserted + " values.");
						}
						catch (HectorException e)
						{
							System.out.println("ERROR: inserting " + key + "[" + timestamp + "] = " + avgSpeed + " FAILED");
						}
					}
					catch (ParseException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}

	/**
	 * Generates a predicted speed for a station and date
	 * by using historical data from the Cassandra database.
	 * @param stationId Unique identifier for the measurement station we're interested in.
	 * @param predictedArrivalTime The time for which we want to get a predicted speed. (Format: MM/DD/YYYY HH:MM:SS)
	 * @return The predicted speed in mph
	 */
	public double getPredictedSpeed(int stationId, String predictedArrivalTime)
	{
		try
		{
			Date predictedArrivalAsDate = dateTemplate.parse(predictedArrivalTime);
			ArrayList<Date> dates = getAllPreviousDates(predictedArrivalAsDate);	
			TreeMap<Long, Double> allSamples = new TreeMap<Long, Double>();
			for (Date d : dates)
			{
				String key = constructKey(stationId, d);
				Date rangeStart = getRangeStartForDate(d);
				Date rangeEnd = getRangeEndForDate(d);
				HashMap<Long, Double> queryResults = sliceQuery(rangeStart, rangeEnd, key);
				allSamples.putAll(queryResults);
			}
			
			for (Long l : allSamples.keySet())
			{
				Date sampleTime = new Date(l);
				System.out.println(sampleTime.toString() + " --> " + allSamples.get(l));
			}
			
			System.out.println("\n----- Simple Average -----");
			System.out.println("Predicted speed: " + simpleAverage(allSamples.values()) + " mph");
			
			System.out.println("\n----- Weighted Average -----");
			System.out.println("Predicted speed: " + weightedAverage(allSamples, predictedArrivalAsDate) + " mph");
			
			System.out.println("\n----- Exponential Smoothing (Alpha = 0.5) -----");
			System.out.println("Predicted speed: " + exponentialSmoothing(allSamples, 0.5) + " mph");
			
			return 0.0;
		}
		catch (ParseException e)
		{
			e.printStackTrace();
			return -1;
		}
	}
	
	/**
	 * Generates a predicted speed value by averaging all values retrieved
	 * from the database.
	 * @param speeds A set of speeds from the database
	 * @return The average of the provided speed set (in mph)
	 */
	public double simpleAverage(Collection<Double> speeds)
	{
		double sum = 0.0;
		
		for (Double d : speeds)
			sum += d;
		
		return sum / (double) speeds.size();
	}
	
	/**
	 * Generates a predicted speed value using a weighted average. Weights are
	 * calculated dynamically using the difference between the requested date
	 * and a sample's timestamp, such that older samples are weighted lower.
	 * @param speeds A mapping of [column name (timestamp)]->[speed]
	 * fetched from the database
	 * @param predictedArrivalAsDate
	 * @return The predicted speed in mph
	 */
	public double weightedAverage(TreeMap<Long, Double> speeds, Date predictedArrivalAsDate)
	{
		double[] weights = new double[speeds.size()];
		double predictedDate = predictedArrivalAsDate.getTime();
		int i = 0;
		double sum = 0;
		for (Long l : speeds.descendingKeySet())
		{			
			weights[i] = 1.0 / (double)(predictedDate - l);
			sum += weights[i];
			++i;
		}
		
		double differenceFromOne = 1 - sum;
		double normalizeFactor = differenceFromOne / speeds.size();
		
		for (int j = 0; j < weights.length; ++j)
		{
			weights[j] += normalizeFactor;
		}
		
		double weightedAvg = 0;
		i = 0;
		for (Long l : speeds.descendingKeySet())
		{
			weightedAvg += speeds.get(l) * weights[i];
			++i;
		}
		
		return weightedAvg;
	}
	
	/**
	 * Generates a predicted speed value using exponential smoothing.
	 * @param speeds A mapping of [column name (timestamp)]->[speed]
	 * fetched from the database
	 * @param alpha The weighting factor
	 * @return The predicted speed in mph
	 */
	public double exponentialSmoothing(TreeMap<Long, Double> speeds, double alpha)
	{		
		int i = 0;
		double prevActualVal = -1;
		double prevSmoothVal = -1;
		for (Long l : speeds.keySet())
		{
			double actualVal = speeds.get(l);
			double smoothVal = (i == 0) ? actualVal : alpha * prevActualVal + (1 - alpha) * prevSmoothVal;
			prevActualVal = actualVal;
			prevSmoothVal = smoothVal;
			++i;
		}
		
		double finalSmoothVal = alpha * prevActualVal + (1 - alpha) * prevSmoothVal;
		
		return finalSmoothVal;
	}
	
	/**
	 * Constructs a list of dates that precede a given date
	 * in exactly 1-week increments. For example, after 9/20/2012,
	 * the next date would be 9/13/2012, followed by 9/6/2012, etc.
	 * @param d The date to start "backtracking" from
	 * @return An ArrayList of Date objects representing the generated dates
	 */
	public ArrayList<Date> getAllPreviousDates(Date d)
	{
		ArrayList<Date> dates = new ArrayList<Date>();
		
		Calendar current = Calendar.getInstance();
		current.setTime(d);
		while (firstDateInDb.before(current))
		{
			dates.add(current.getTime());
			current.add(Calendar.DATE, -7);
		}
		
		return dates;
	}
	
	/**
	 * Computes a date exactly 1 week prior to a given date.
	 * @param d The Date to "backtrack" from
	 * @return A Date exactly 1 week before the given date
	 */
	public Date getDateOneWeekPriorToDate(Date d)
	{
		System.out.println("Given date: " + d.toString());
		Calendar cal = Calendar.getInstance();
		cal.setTime(d);
		cal.add(Calendar.DATE, -7);
		System.out.println("One week prior: " + cal.getTime().toString());
		
		return cal.getTime();
	}
	
	/**
	 * Given a Date, computes a Date slightly before it that will
	 * act as the lower bound for a range query against the
	 * Cassandra database.
	 * @param d The Date representing the range query's "midpoint".
	 * @return A Date 15 minutes earlier than the given Date.
	 */
	public Date getRangeStartForDate(Date d)
	{
		Calendar c = Calendar.getInstance();
		c.setTime(d);
		c.add(Calendar.MINUTE, -15);
		return c.getTime();
	}
	
	/**
	 * Given a Date, computes a Date slightly after it that will
	 * act as the upper bound for a range query against the
	 * Cassandra database.
	 * @param d The Date representing the range query's "midpoint".
	 * @return A Date 15 minutes later than the given Date.
	 */
	public Date getRangeEndForDate(Date d)
	{
		Calendar c = Calendar.getInstance();
		c.setTime(d);
		c.add(Calendar.MINUTE, 15);
		return c.getTime();
	}
	
	/**
	 * Executes a range (slice) query on the Cassandra database.
	 * @param timestamp1 The low end of the timestamp range (in Long form)
	 * @param timestamp2 The high end of the timestamp range (in Long form)
	 * @param key The Cassandra key for the row to retrieve
	 * @return A map of the form [timestamp]->[speed] containing all speeds
	 * (and their respective column names) within the given range
	 */
	public HashMap<Long, Double> sliceQuery(Date timestamp1, Date timestamp2, String key)
	{
		HashMap<Long, Double> resultsMap = new HashMap<Long, Double>();
		
		SliceQuery<String, Long, Double> q = HFactory.createSliceQuery(ksp, StringSerializer.get(), LongSerializer.get(), DoubleSerializer.get());
		q.setColumnFamily(COLUMN_FAMILY_NAME);
		q.setKey(key);
		
		q.setRange(timestamp1.getTime(), timestamp2.getTime(), false, Integer.MAX_VALUE);
		QueryResult<ColumnSlice<Long, Double>> resultSet = q.execute();
		
		ColumnSlice<Long, Double> slice = resultSet.get();
		for (HColumn<Long, Double> c : slice.getColumns())
		{
			resultsMap.put(c.getName(), c.getValue());
		}
		
		return resultsMap;
	}
	
	/**
	 * Constructs a Cassandra row key.
	 * @param stationId The station ID component of the key
	 * @param date The timestamp component of the key
	 * @return A String key of the form [stationId]:[MM/DD/YYYY]
	 */
	public String constructKey(int stationId, Date date)
	{
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int year = cal.get(Calendar.YEAR);
		int month = cal.get(Calendar.MONTH);
		int day = cal.get(Calendar.DAY_OF_MONTH);
		
		return stationId + ":" + month + "/" + day + "/" + year;
	}
	
	/*
	 * ================================================
	 * NOTE: the following methods were written for initial
	 * testing purposes. I've kept them because their code
	 * may be useful as a reference, but they may not work
	 * correctly with the current schema. The methods designed
	 * to test insertions, especially, should not be called.
	 * ================================================
	 */
	
	@Deprecated
	public void testSliceQuery() throws ParseException
	{
		//SimpleDateFormat sdf = new SimpleDateFormat("MM'/'dd'/'yyyy' 'HH':'mm':'ss");
		
		SliceQuery<String, Long, Double> q = HFactory.createSliceQuery(ksp, StringSerializer.get(), LongSerializer.get(), DoubleSerializer.get());
		q.setColumnFamily(COLUMN_FAMILY_NAME);
		q.setKey(constructKey(400014, dateTemplate.parse("09/12/2012 17:05:00")));
			
		Date date1 = dateTemplate.parse("09/12/2012 17:05:00");
		Date date2 = dateTemplate.parse("09/12/2012 17:15:00");
		
		q.setRange(date1.getTime(), date2.getTime(), false, 10);
		QueryResult<ColumnSlice<Long, Double>> resultSet = q.execute();
		
		ColumnSlice<Long, Double> slice = resultSet.get();
		for (HColumn<Long, Double> c : slice.getColumns())
		{
			System.out.println(c.getName() + " : " + c.getValue());
		}
	}
	
	@Deprecated
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
		
//		ColumnFamilyUpdater<String, Integer> updater = template.createUpdater("a key");
//		updater.setDouble(0, 54.7);
//		updater.setDouble(1, 23.5);
//		updater.setDouble(2, 77.0);
//		updater.setDouble(3, 45.0);
//		
//		try
//		{
//			template.update(updater);
//		}
//		catch (HectorException e)
//		{
//			
//		}
	}
	
	@Deprecated
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
//		
//		SliceQuery<String, Integer, Double> q = HFactory.createSliceQuery(ksp, StringSerializer.get(), IntegerSerializer.get(), DoubleSerializer.get());
//		q.setColumnFamily(COLUMN_FAMILY_NAME);
//		q.setKey("a key");
//		q.setRange(1, 3, false, 4);
//		QueryResult<ColumnSlice<Integer, Double>> resultSet = q.execute();
//		ColumnSlice<Integer, Double> slice = resultSet.get();
//		for (HColumn<Integer, Double> c : slice.getColumns())
//		{
//			System.out.println(c.getName() + " : " + c.getValue());
//		}
	}
	
	@Deprecated
	public void testReadTrafficData() throws ParseException
	{
		//SimpleDateFormat sdf = new SimpleDateFormat("MM'/'dd'/'yyyy' 'HH':'mm':'ss");
		Date date = dateTemplate.parse("09/01/2012 03:25:00");
		long timestamp = date.getTime();
		
		Date date2 = dateTemplate.parse("09/20/2012 21:10:00");
		long timestamp2 = date2.getTime();
		
		String key = constructKey(402640, date);
		
		String key2 = constructKey(400576, date2);
		
		try
		{
		    ColumnQuery<String, Long, Double> query = HFactory.createColumnQuery(ksp,
		    		StringSerializer.get(),
		    		LongSerializer.get(),
		    		DoubleSerializer.get());
		    query.setColumnFamily(COLUMN_FAMILY_NAME).setKey(key).setName(timestamp);
		    QueryResult<HColumn<Long, Double>> result = query.execute();
		    HColumn<Long, Double> col = result.get();
		    if (col.getValue() == 68.9)
		    	System.out.println("Read test successful!");
		    else
		    	System.out.println("Read test FAILED!");
		    
		    ColumnQuery<String, Long, Double> query2 = HFactory.createColumnQuery(ksp,
		    		StringSerializer.get(),
		    		LongSerializer.get(),
		    		DoubleSerializer.get());
		    query2.setColumnFamily(COLUMN_FAMILY_NAME).setKey(key2).setName(timestamp2);
		    QueryResult<HColumn<Long, Double>> result2 = query2.execute();
		    HColumn<Long, Double> col2 = result2.get();
		    if (col2.getValue() == 63.3)
		    	System.out.println("Read test successful!");
		    else
		    	System.out.println("Read test FAILED!");
		}
		catch (HectorException e) {
		    // do something ...
		}
	}
}

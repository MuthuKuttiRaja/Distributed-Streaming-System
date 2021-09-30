import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import twitter4j.FilterQuery;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;


class SpoutClass
{
	public String timeStamp()
	{
		String timeStamp = new SimpleDateFormat("HH.mm.ss.SSSS").format(new Date());
		return timeStamp;
	}
	public Date convertDateObj(String str)
	{
		Date date = null;
		try
		{
		String expectedPattern = "HH.mm.ss.SSSS";
	    SimpleDateFormat formatter = new SimpleDateFormat(expectedPattern);
	    date = formatter.parse(str);
		}
		catch(Exception e)
		{
			
		}
		 return date;
	}
	public void Execute(String localID) throws InterruptedException
	{
		SpoutClass obj = new SpoutClass();
		int index=0;
		
		for(int i=0;i<100;i++)
		{
			Workers.GeneratedSpoutValues++;
			index++;
			System.out.println("Generated Values: " + "Key" +"_" + index  + " " + "value" );
			//Thread.sleep(5);
			//System.out.println(System.currentTimeMillis() + ":" +"Key" +"_" +index++  + " " + "value" );
			String timeStamp = obj.timeStamp();
			synchronized (Workers.OutThreadMap) {
				Workers.OutThreadMap.put(index + " " +localID,"Key" +"_" +index  + " " + "value");
			}
			System.out.println(index + " " +localID);
		}
	}
}
public class Spout_Bolt_Classes{
	
}

class BoltClass1  {
	
	public String Execute(String localId) throws InterruptedException
	{
		int msgCountIndex = 0;
		while(true)
		{
			String receivedVal =Workers.retreiveTuples(localId);
    		if(receivedVal!=null)
    		{
    			String value = receivedVal.substring(receivedVal.indexOf(' ')+1, receivedVal.length());
    			String[] splits = value.split(" ");
    			value ="";
    			for(int i =1;i<splits.length;i++)
    				value+=splits[i]+" ";
    			System.out.println("Class: " +this.getClass().getName() +" \t Key:" + splits[0] + "\tValues: " +value);
    			synchronized (Workers.OutThreadMap) {
    				Workers.OutThreadMap.put((msgCountIndex++) +" " +localId,splits[0]  + " " + value);
				}
    		}
    		else
    			Thread.sleep(50);
		}
		
	}
}

class BoltClass2 {
	
	
	public String Execute(String localId) throws InterruptedException
	{
		int msgCountIndex = 0;
		while(true)
		{
			String receivedVal =Workers.retreiveTuples(localId);
    		if(receivedVal!=null)
    		{
    			String value = receivedVal.substring(receivedVal.indexOf(' ')+1, receivedVal.length());
    			String[] splits = value.split(" ");
    			value ="";
    			for(int i =1;i<splits.length;i++)
    				value+=splits[i]+" ";
    			System.out.println("Class: " +this.getClass().getName() +" \t Key:" + splits[0] + "\tValues: " +value);
    			synchronized (Workers.OutThreadMap) {
    				Workers.OutThreadMap.put((msgCountIndex++) +" " +localId,splits[0]  + " " + value);
				}
    		}
    		else
    			Thread.sleep(50);
		}
		
	}
}

class BoltClass3 {
	
	
	public String Execute(String localId) throws InterruptedException
	{
		int msgCountIndex = 0;
		while(true)
		{
			String receivedVal =Workers.retreiveTuples(localId);
    		if(receivedVal!=null)
    		{
    			String value = receivedVal.substring(receivedVal.indexOf(' ')+1, receivedVal.length());
    			String[] splits = value.split(" ");
    			value ="";
    			for(int i =1;i<splits.length;i++)
    				value+=splits[i]+" ";
    			System.out.println("Class: " +this.getClass().getName() +" \t Key:" + splits[0] + "\tValues: " +value);
    			synchronized (Workers.OutThreadMap) {
    				Workers.OutThreadMap.put((msgCountIndex++) +" " +localId,splits[0]  + " " + value);
				}
    		}
    		else
    			Thread.sleep(50);
		}
		
	}
}





class FilereaderClass
{
	public void Execute(String localID) throws InterruptedException
	{
		try 
		{
			int index = 0;
			String str;
			FileReader fileReader = new FileReader("11.txt");
			BufferedReader reader = new BufferedReader(fileReader);
			while ((str = reader.readLine()) != null) {
				if(!str.equals(""))
				{
					synchronized (Workers.OutThreadMap) {
					    //The value stored in map is of format key + " " + value for the next task to process
							Workers.OutThreadMap.put((index++) + " " +localID,index + " " + str);
					}
				}
			}
		} 
		catch (Exception e) 
		{
			throw new RuntimeException("Error reading typle", e);
		} 
		
	}
}


class WordSpitterClass {
	public String Execute(String localId) throws InterruptedException
	{
		int msgCountIndex = 0;
		while(true)
		{
			String receivedVal =Workers.retreiveTuples(localId);
    		if(receivedVal!=null)
    		{
    			String value = receivedVal.substring(receivedVal.indexOf(' ')+1, receivedVal.length());
    			String[] splits = value.split(" ");
    			value ="";
    			for(int i =1;i<splits.length;i++)
    			{
    				synchronized (Workers.OutThreadMap) {
    					if(!splits[i].equals(""))
    					{
    						Workers.OutThreadMap.put((msgCountIndex++) +" " +localId,splits[0]  + " " + splits[i]);
    					}
    				}
    			}
    		}
    		else
    			Thread.sleep(50);
		}
	}
}


class WordMergerClass {
	public String Execute(String localId) throws InterruptedException
	{
		Map<String,Long> map = new HashMap<String, Long>();
		while(true)
		{
			String receivedVal =Workers.retreiveTuples(localId);
    		if(receivedVal!=null)
    		{
    			String value = receivedVal.substring(receivedVal.indexOf(' ')+1, receivedVal.length());
    			String[] splits = value.split(" ");
    			value ="";
    			if(map.containsKey(splits[1]))
    			{
    				map.put(splits[1],map.get(splits[1])+1);
    				System.out.println("Value: " + splits[1]  +", Count: " + map.get(splits[1]));
    			}
    			else
    			{
    				map.put(splits[1],(long) 1);
    				System.out.println("Value: " + splits[1]  +", Count: " + map.get(splits[1]));
    			}
    		}
    		else
    			Thread.sleep(50);
		}
	}
}



class TwitterStreamClass
{
	public void Execute(String localID) throws InterruptedException
	{
	    ConfigurationBuilder cb = new ConfigurationBuilder();
	    cb.setDebugEnabled(true);
	    cb.setOAuthConsumerKey("");
	    cb.setOAuthConsumerSecret("");
	    cb.setOAuthAccessToken("");
	    cb.setOAuthAccessTokenSecret("");

	    TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
	    StatusListener listener = new StatusListener() {
	    	int index = 0;
	        public void onStatus(Status status) {
	        	 //System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText() + " -> "+ status.getCreatedAt());
	        	 HashtagEntity[] hashTags= status.getHashtagEntities();
				 ArrayList<String> aL= new ArrayList<String>();
				 for(HashtagEntity individualTag:hashTags)
				 {
					 System.out.println("#"+individualTag.getText().toLowerCase());
					 synchronized (Workers.OutThreadMap) {
						    //The value stored in map is of format key + " " + value for the next task to process
							Workers.OutThreadMap.put((index++) + " " +localID,individualTag.getText().toLowerCase() + " " + status.getUser().getScreenName());
					}
				 } 
	        }

	        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
	            System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
	            
	        }

	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
	            System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
	        }

	        public void onScrubGeo(long userId, long upToStatusId) {
	            System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
	        }

	        public void onException(Exception ex) {
	            ex.printStackTrace();
	        }

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				
			}
	    };

	    FilterQuery fq = new FilterQuery();
	    String keywords[] = {"France", "Germany"};

	    fq.track(keywords);

	    twitterStream.addListener(listener);
	    twitterStream.filter(fq);      

	}
}

class TwitterBoltMergeClass
{
	public String Execute(String localId) throws InterruptedException
	{
		Map<Long,Map<String,ArrayList<String>>> map = new HashMap<Long, Map<String,ArrayList<String>>>();
		int msgCountIndex = 0;
		while(true)
		{
			String receivedVal =Workers.retreiveTuples(localId);
    		if(receivedVal!=null)
    		{
    			String value = receivedVal.substring(receivedVal.indexOf(' ')+1, receivedVal.length());
    			String[] splits = value.split(" ");
    			value ="";
    			for(int i =1;i<splits.length;i++)
    				value+=splits[i]+" ";
    			
    			if(map.size()==0)
    			{
    				ArrayList<String> aL = new ArrayList<String>();
    				aL.add(value);
    				Map<String,ArrayList<String>> innerMap = new HashMap<String, ArrayList<String>>();
    				innerMap.put(splits[0], aL);
    				map.put(System.currentTimeMillis(),innerMap);
    			}
    			else
    			{
    				Long mapkey = new ArrayList<Long>(map.keySet()).get(0);
    				if( (System.currentTimeMillis() - mapkey) >=20000 )
    				{
    					//Flush the map output
    					for(Map.Entry<String, ArrayList<String>> entry:map.get(mapkey).entrySet())
    					{
    						String innerValue = "";
    						for(String s:entry.getValue())
    							innerValue+=s+" ";
    						synchronized (Workers.OutThreadMap) {
    		    				Workers.OutThreadMap.put((msgCountIndex++) +" " +localId,entry.getKey()  + " " + innerValue);
    						}
    					}
    					map.remove(mapkey);
    				}
    				else
    				{
    					if(map.get(mapkey).containsKey(splits[0]))
    					{
    						map.get(mapkey).get(splits[0]).add(value);
    					}
    					else
    					{
    						ArrayList<String> aL = new ArrayList<String>();
    						aL.add(value);
    						map.get(mapkey).put(splits[0],aL);
    					}
    				}
    			}
    		}
    		else
    			Thread.sleep(50);
		}
		
	}
}


class TwitterReportBolt {
	public String Execute(String localId) throws InterruptedException
	{
		int msgCountIndex = 0;
		while(true)
		{
			String receivedVal =Workers.retreiveTuples(localId);
    		if(receivedVal!=null)
    		{
    			String value = receivedVal.substring(receivedVal.indexOf(' ')+1, receivedVal.length());
    			String[] splits = value.split(" ");
    			value ="";
    			for(int i =1;i<splits.length;i++)
    				value+=splits[i]+" ";
    			
    			System.out.println("HashTagIndex: " + (msgCountIndex++) +" \n HashTag:" + splits[0] + "\nUsers: " +value+"\n");
    			synchronized (Workers.OutThreadMap) {
    				Workers.OutThreadMap.put((msgCountIndex++) +" " +localId,splits[0]  + " " + value);
				}
    		}
    		else
    			Thread.sleep(50);
		}
	}
}


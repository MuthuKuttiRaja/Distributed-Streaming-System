import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Nimbus extends Thread {
	public static ArrayList<String> NetworkInfo = new ArrayList<String>();
	public static Map<String,TopologyValues> LocalTopoInfo = new HashMap<String, TopologyValues>();
	public static Map<String,TopologyValues> NextTopoInfo = new ConcurrentHashMap<String, TopologyValues>();
	public static int totalExecutorsReq;
	public static Map<String,ArrayList<String>> NetworkMapTasks = new HashMap<String, ArrayList<String>>();
	public static long tupleCount = 0;
	
	public static final Logger logger =
	        Logger.getLogger(Nimbus.class.getName());
	
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
	
	public void run()
	{
		try
		{
			FileHandler macfileHandler = new FileHandler("logger_Nimbus.log");
			macfileHandler.setFormatter(new SimpleFormatter());
			logger.addHandler(macfileHandler);
			logger.setUseParentHandlers(false);
			
			DatagramSocket serverSocket = new DatagramSocket(53609);
			serverSocket.getInetAddress();
			String ipAddress = InetAddress.getLocalHost().toString();
			ipAddress = ipAddress.substring(ipAddress.lastIndexOf("/")+1, ipAddress.length());
			System.out.println("Nimbus waiting at port  "
					+ serverSocket.getLocalPort() + "  in IP address"
					+ ipAddress);
			ThreadPool tP = new ThreadPool(serverSocket);
			tP.start();
		}
		catch(IOException e)
		{
			
		}
	}
	
	public void AssignNodesToTasks()
	{
		Random rn = new Random();
		for(Map.Entry<String, TopologyValues> entry: LocalTopoInfo.entrySet())
		{
			Set<String> IPAddressSet = new HashSet<String>();
			
			do
			{
				int randomNetworkId = rn.nextInt(NetworkInfo.size());
				IPAddressSet.add(NetworkInfo.get(randomNetworkId));
			}while(IPAddressSet.size()<entry.getValue().getParallelism());
			//System.out.println("Hi");
			
			for(String info:IPAddressSet)
			{
				if(NetworkMapTasks.containsKey(info))
				{
					ArrayList<String> aL = new ArrayList<String>();
					aL.addAll(NetworkMapTasks.get(info));
					aL.add(entry.getKey());
					NetworkMapTasks.put(info, aL);
				}
				else
				{
					ArrayList<String> aL = new ArrayList<String>();
					aL.add(entry.getKey());
					NetworkMapTasks.put(info, aL);
				}
			}
		}
		
		
		for(Map.Entry<String, TopologyValues> entry:NextTopoInfo.entrySet())
		{
			ArrayList<String> IPList = new ArrayList<String>();
			for(Map.Entry<String, ArrayList<String>> innerEntry:NetworkMapTasks.entrySet())
			{
				if(innerEntry.getValue().contains(entry.getValue().getNextTaskId()))
				{
					IPList.add(innerEntry.getKey());
				}
			}
			TopologyValues topoVal = entry.getValue();
			topoVal.setNodeList(IPList);
			NextTopoInfo.remove(entry.getKey());
			NextTopoInfo.put(entry.getKey(), topoVal);
		}
	}
	
	public String ReqRouteInfo(String Key,String localInfo)
	{
		String reqRouteInfo ="";
		TopologyValues val = NextTopoInfo.get(localInfo);
		
		if(val==null)
			return "null";
		
		String groupingStyle = val.getGrouping();
		ArrayList<String> nodeList = val.getNodeList();
		Random rand = new Random();
		if(groupingStyle.equals("shufflegrouping"))
		{
			int entry = rand.nextInt(nodeList.size());
			reqRouteInfo = nodeList.get(entry);
		}
		else if(groupingStyle.equals("allgrouping"))
		{
			reqRouteInfo = nodeList.get(0);
		}
		else if(groupingStyle.equals("fieldsgrouping"))
		{
			int entry = Math.abs(Key.hashCode())%nodeList.size();
			reqRouteInfo = nodeList.get(entry);
		}
		reqRouteInfo = val.getNextTaskId() + " " + reqRouteInfo;
		return reqRouteInfo;
	}
	
	public void readConfFile(String fileName)
	{
		try
		{
		File file = new File(fileName);
		RandomAccessFile rac = new RandomAccessFile(file, "r");
		byte[] fileContents = new byte[(int) file.length()];
		rac.read(fileContents);
		String contents = new String(fileContents);
		String[] contentSplits = contents.split("\n");
		Map<String,String> map = new HashMap<String, String>();
		
		for(int i = 0;i<contentSplits.length ;i++)
		{
			TopologyValues topoVal = new TopologyValues();
			String[] val = contentSplits[i].substring(contentSplits[i].indexOf('(')+1, contentSplits[i].indexOf(')')).split(",");
			if(i>0)
			{
				//topoVal.setClassName(map.get(val[4]));
				topoVal.setGrouping(val[3]);
				topoVal.setNextTaskId(val[0]);
				topoVal.setParallelism(Integer.parseInt(val[2]));
				NextTopoInfo.put(val[4], topoVal);
			}
			
			topoVal = new TopologyValues();
			topoVal.setClassName(val[1]);
			if(i!=0)
			{
				totalExecutorsReq+=Integer.parseInt(val[2]);
				topoVal.setParallelism(Integer.parseInt(val[2]));
			}
			else
			{
				totalExecutorsReq+=1;
				topoVal.setParallelism(1);
			}
			LocalTopoInfo.put(val[0], topoVal);
		}
		
		
		}
		catch(IOException e)
		{
			
		}
		
	}
	public String legalLength(String val)
	{
		String length ="";
		String appendZeros = "";
		for(int i=0;i<(4 - val.length());i++)
			appendZeros+="0";
		length = appendZeros+val;
		return length;
	}
	
	public void RequestExecutorsCreation()
	{
		Nimbus nimObj = new Nimbus();
		String message = "";
		for(Map.Entry<String, ArrayList<String>> entry: NetworkMapTasks.entrySet())
		{
			String completeIDTasks ="";
			for(String taskId:entry.getValue())
			{
				if(!taskId.equals("spout"))
					completeIDTasks+=taskId+" " + LocalTopoInfo.get(taskId).getClassName()+" ";
			}
			if(!entry.getValue().contains("spout"))
				message = " CREATEEXEC " +entry.getValue().size() + " " +completeIDTasks;
			else
				message = " CREATEEXEC " + (entry.getValue().size()-1) + " " +completeIDTasks;
			message=nimObj.legalLength(message.length()+"")+message;
			nimObj.packetInterchange(message, entry.getKey().split(" ")[0], Integer.parseInt(entry.getKey().split(" ")[1]), false);
		}
		for(Map.Entry<String, ArrayList<String>> entry: NetworkMapTasks.entrySet())
		{
			String completeIDTasks ="";
			for(String taskId:entry.getValue())
			{
				if(taskId.equals("spout"))
				{
					completeIDTasks+=taskId+" " + LocalTopoInfo.get(taskId).getClassName()+" ";
					message = " STARTSPOUT " +completeIDTasks;
					message=nimObj.legalLength(message.length()+"")+message;
					nimObj.packetInterchange(message, entry.getKey().split(" ")[0], Integer.parseInt(entry.getKey().split(" ")[1]), false);
					break;
				}
			}
			
		}
		
//		for(Map.Entry<String, ArrayList<String>> entry:NetworkMapTasks.entrySet())
//		{
//			System.out.println(entry.getKey() + " " + entry.getValue());
//		}
//		for(Map.Entry<String, TopologyValues> entry:NextTopoInfo.entrySet())
//		{
//			System.out.println(entry.getValue().toString());
//		}
	}
	public String packetInterchange(String sendMessage,String ipInfoAddress,int nodePort,boolean receiveFlag)
	{
		String receivedMsg="";
		try
		{
		InetAddress	nodeAddress = InetAddress.getByName(ipInfoAddress);
		byte[] sendData = new byte[sendMessage.length()];
		sendData= sendMessage.getBytes();
		byte[] receiveJoinMessage = new byte[1024];
		DatagramSocket sock = new DatagramSocket(); 
		//sock.setSoTimeout(500);
		DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, nodeAddress, nodePort);   
		sock.send(sendPacket); 
		//System.out.println("The packet Message: " +sendMessage + " to Address Info: "+nodeAddress+":"+nodePort);
		if(receiveFlag == true)
		{
			DatagramPacket receivePacket = new DatagramPacket(receiveJoinMessage, receiveJoinMessage.length); 
			sock.receive(receivePacket); 
			receivedMsg= new String(receivePacket.getData());
			receivedMsg = receivedMsg.substring(0,receivePacket.getLength());
		}
		sock.close();
		}
		catch(InterruptedIOException e)
		{
			receivedMsg = null;
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		return receivedMsg;
	}
	
	public static void main(String args[]) {
		Nimbus obj = new Nimbus();
		obj.start();
		System.out.println("1) Enter the Path of configuration File");
		Scanner scanner = new Scanner(System.in);
		while(true)
		{
			int option = scanner.nextInt();
			if(option==1)
			{
				String confFile = scanner.next();
				confFile = confFile.trim();
				obj.readConfFile(confFile);
				obj.AssignNodesToTasks();
				obj.RequestExecutorsCreation();
			}
		}
	}
}

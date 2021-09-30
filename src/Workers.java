import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Workers extends Thread {
	public static long GeneratedSpoutValues=0;
	public static String NimbusIPaddress;
	public static int NimbusPortNo;
	public static Map<String,String> ReceivingThreadMap = new ConcurrentHashMap<String, String>();
	public static Map<String,String> OutThreadMap = new ConcurrentHashMap<String, String>();
	
	public static String retreiveTuples(String localId)
	{
		String val = null;
		synchronized (Workers.ReceivingThreadMap) {
			for(Map.Entry<String, String> entry:Workers.ReceivingThreadMap.entrySet())
			{
				if(entry.getKey().split(" ")[1].equals(localId))
				{
					val = entry.getValue();
					Workers.ReceivingThreadMap.remove(entry.getKey());
					break;
				}
			}
		}
		return val;
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
		sock.setSoTimeout(500);
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
	public void run()
	{
		try
		{
			Workers wObj = new Workers();
			DatagramSocket serverSocket = new DatagramSocket(0);
			serverSocket.getInetAddress();
			String ipAddress = InetAddress.getLocalHost().toString();
			ipAddress = ipAddress.substring(ipAddress.lastIndexOf("/")+1, ipAddress.length());
			int localPort = serverSocket.getLocalPort();
			System.out.println("Workers waiting at port  "
					+ localPort + "  in IP address"
					+ ipAddress);
			
			//First Time registration with nimbus
			String message = " JOIN " + ipAddress + " " + localPort;
			message = wObj.legalLength(message.length()+"")+message;
			String responseMsg = packetInterchange(message, NimbusIPaddress, NimbusPortNo, true);
			System.out.println(responseMsg);
			ListeningWorkerThread lWT = new ListeningWorkerThread(serverSocket);
			lWT.start();
		}
		catch(IOException e)
		{
			
		}
	}
	public static void main(String args[])
	{
		NimbusIPaddress=args[0];
		NimbusPortNo=Integer.parseInt(args[1]);
		Workers obj = new Workers();
		obj.start();
	}
}

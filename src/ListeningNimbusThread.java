import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ListeningNimbusThread extends Thread {
	
	private DatagramPacket receivePacket;
	private DatagramSocket serverSocket;
	public ListeningNimbusThread(DatagramPacket receivePacket,DatagramSocket serverSocket)
	{
		this.receivePacket = receivePacket;
		this.serverSocket = serverSocket;
	}
	
	public void run()
	{
		try
		{
			byte[] sendData = new byte[1024];
			InetAddress receivedIPAddress = receivePacket.getAddress();
			int receivedPortNo = receivePacket.getPort();
			Nimbus nimObj = new Nimbus();
			String receiveMsg = new String(receivePacket.getData());
			receiveMsg= receiveMsg.substring(0,receivePacket.getLength());
			String[] receiveMsgSplits = receiveMsg.split(" ");
			//System.out.println("Received: " + receiveMsg);
			if(receiveMsgSplits[1].equals("JOIN"))
			{
				Nimbus.NetworkInfo.add(receiveMsgSplits[2] + " " + receiveMsgSplits[3]);
				String sendResponseMsg ="0006 JOINOK";
				sendData = sendResponseMsg.getBytes();
				DatagramPacket sendPacket = new DatagramPacket(sendData,
						sendData.length, receivedIPAddress, receivedPortNo);
				serverSocket.send(sendPacket);
			}
			else if(receiveMsgSplits[1].equals("REQROUTE"))
			{
				String requestedID = receiveMsgSplits[2];
				String key = receiveMsgSplits[3];
				String reqRouteInfo = nimObj.ReqRouteInfo(key, requestedID);
				reqRouteInfo = nimObj.legalLength(reqRouteInfo.length()+"") + " " + reqRouteInfo;
				sendData = reqRouteInfo.getBytes();
				DatagramPacket sendPacket = new DatagramPacket(sendData,
						sendData.length, receivedIPAddress, receivedPortNo);
				serverSocket.send(sendPacket);
				Nimbus.logger.info("Tuple Count: " + Nimbus.tupleCount++ + "TimeStamp: " + nimObj.timeStamp());
				//System.out.println("Tuple Count: " + Nimbus.tupleCount++ + "TimeStamp: " + nimObj.timeStamp());
				//System.out.println("Sent Packet: " + reqRouteInfo);
			}
		
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}

}

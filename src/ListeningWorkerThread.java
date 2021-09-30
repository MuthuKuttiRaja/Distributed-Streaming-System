import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;


public class ListeningWorkerThread extends Thread {

	
	private DatagramSocket serverSocket;

	public ListeningWorkerThread(DatagramSocket serverSocket) {
		this.serverSocket = serverSocket;
	}
	public void run()
	{
		WorkerSendThread wSt = new WorkerSendThread();
		wSt.start();
		int msgCount =0;
		try
		{
		byte[] receiveData = new byte[1024];
		while (true) {
			DatagramPacket receivePacket = new DatagramPacket(receiveData,
					receiveData.length);
			serverSocket.receive(receivePacket);
			byte[] sendData = new byte[1024];
			InetAddress receivedIPAddress = receivePacket.getAddress();
			int receivedPortNo = receivePacket.getPort();
			Workers workObj = new Workers();
			String receiveMsg = new String(receivePacket.getData());
			
			//System.out.println("Received: " +receiveMsg);
			receiveMsg= receiveMsg.substring(0,receivePacket.getLength());
			String[] receiveMsgSplits = receiveMsg.split(" ");
			if(receiveMsgSplits[1].equals("CREATEEXEC"))
			{
				//Create Executors
				for(int i=0;i<Integer.parseInt(receiveMsgSplits[2]);i++)
				{
					UserLogicThread uLT = new UserLogicThread(receiveMsgSplits[3+(i*2)], receiveMsgSplits[4+(i*2)],false);
					uLT.start();
				}
			}
			else if(receiveMsgSplits[1].equals("STARTSPOUT"))
			{
				UserLogicThread uLT = new UserLogicThread(receiveMsgSplits[2], receiveMsgSplits[3],true);
				uLT.start();
			}
			else if(receiveMsgSplits[1].equals("RECVMSG"))
			{
				String val = "";
				for(int i=2;i<receiveMsgSplits.length;i++)
					val+=receiveMsgSplits[i]+" ";
				val=val.trim();
				
				synchronized (Workers.ReceivingThreadMap) {
					Workers.ReceivingThreadMap.put( (msgCount++) +" " + receiveMsgSplits[2] ,val);
				}
				
			}
			
			
		}
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}


}

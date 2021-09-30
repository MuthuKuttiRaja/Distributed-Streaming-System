import java.util.Map;


public class WorkerSendThread extends Thread{
	
	public WorkerSendThread()
	{
		
	}
	public String GetOutValues()
	{
		synchronized (Workers.OutThreadMap.entrySet()) {
			for(Map.Entry<String, String> entry:Workers.OutThreadMap.entrySet())
			{
				String val = entry.getKey().split(" ")[1] + " " + entry.getValue();
				Workers.OutThreadMap.remove(entry.getKey());
				return val;
			}
		}
		return null;
	}
	public void run()
	{
		try
		{
			WorkerSendThread wST = new WorkerSendThread();
			Workers workerObj = new Workers();
			String outgoingMessage ="";
			while(true)
			{
				String info = wST.GetOutValues();
				outgoingMessage = info;
				if(outgoingMessage==null)
				{
					Thread.sleep(50);
					continue;
				}
				outgoingMessage = " REQROUTE " + outgoingMessage;
				outgoingMessage = workerObj.legalLength(outgoingMessage.length()+"")+outgoingMessage;
				String receivedMsg = workerObj.packetInterchange(outgoingMessage, Workers.NimbusIPaddress, Workers.NimbusPortNo, true);
				if(receivedMsg!=null)
				{
					String[] receivedMsgSplits = receivedMsg.split(" ");
					if(!receivedMsgSplits[1].equals("null"))
					{
					info = info.substring(info.indexOf(' ')+1, info.length());
					String sendTuplesMsg = " RECVMSG " + receivedMsgSplits[1] +" " +  info;
					sendTuplesMsg = workerObj.legalLength(sendTuplesMsg.length()+"") + sendTuplesMsg;
					workerObj.packetInterchange(sendTuplesMsg, receivedMsgSplits[2], Integer.parseInt(receivedMsgSplits[3]), false);
					}
					else
					{
					
					}
				}
			}
		}
		catch(InterruptedException e)
		{
			e.printStackTrace();
		}
		
	}
}

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPool extends Thread {
	private DatagramSocket serverSocket;

	public ThreadPool(DatagramSocket serverSocket) {
		this.serverSocket = serverSocket;
	}
	public void run() {
		try
		{
		byte[] receiveData = new byte[1024];
		ExecutorService executor = Executors.newFixedThreadPool(10);
		while (true) {
			DatagramPacket receivePacket = new DatagramPacket(receiveData,
					receiveData.length);
			serverSocket.receive(receivePacket);
			Runnable worker = new ListeningNimbusThread(receivePacket,serverSocket);
			executor.execute(worker);
		}
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}
}

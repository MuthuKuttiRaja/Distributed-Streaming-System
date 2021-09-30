import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class Test {
	public static void Run()
	{try 
	{
		int index = 0;
		String str;
		FileReader fileReader = new FileReader("/Users/apple/Downloads/sampleData/11.txt");
		BufferedReader reader = new BufferedReader(fileReader);
		while ((str = reader.readLine()) != null) {
			if(!str.equals(""))
			System.out.println(str);
		}
	} 
	catch (Exception e) 
	{
		throw new RuntimeException("Error reading typle", e);
	} }
	

	public static void main(String args[])
	{
		Test.Run();
	}
}

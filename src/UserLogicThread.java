import java.lang.reflect.Method;
import java.util.Map;


public class UserLogicThread extends Thread {
	private String localId;
	private String ClassName;
	private boolean SpoutFlag;
	public UserLogicThread()
	{
		
	}
	public UserLogicThread(String localId,String ClassName, boolean SpoutFlag)
	{
		this.ClassName = ClassName;
		this.localId = localId;
		this.SpoutFlag = SpoutFlag;
	}
	
	public void run()
	{
		try
		{
        Class<?> ClassTasks = Class.forName(ClassName); // convert string classname to class
        Object taskObj = ClassTasks.newInstance(); // invoke empty constructor
        String methodName = "Execute";
        Class<?>[] paramTypesSpout = {String.class};
        Method setNameMethod = taskObj.getClass().getMethod(methodName,paramTypesSpout);
        setNameMethod.invoke(taskObj,localId); // pass arg	
		}
		catch(Exception e)
		{
			
		}
	}
}

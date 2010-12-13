import java.util.Timer;
import java.util.TimerTask;


public class testfortimer extends TimerTask{

	
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("sfsdfsdf");
		
	}
	public static void main(String args[])
	{
		Timer t=new Timer();
		t.schedule(new testfortimer(), 5,5);

	}
	

}

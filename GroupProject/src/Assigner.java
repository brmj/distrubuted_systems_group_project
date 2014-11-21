import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Date;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;


public class Assigner {
	
	String queueFromAssigner;
	String queueFromServer;
	String jobId;
	String initialConnectionQueue;
	String ipAddressRabbitMqServer;
	
	class InputtoServer {
		
	}
	
	Assigner(String ipAddress) {
		//ip address of rabbitmq server
		this.ipAddressRabbitMqServer = ipAddress;
		this.initialConnectionQueue = "assignerQueue";
		//form myQueue as, assigner + hash value of (localhost + current date time)
		InetAddress ip;
		Date currentDate = new Date();
		try {
			ip = InetAddress.getLocalHost();
			this.queueFromAssigner = ip + currentDate.toString();
			this.jobId = Integer.toString(this.queueFromAssigner.hashCode());
			this.queueFromServer = "FS" + Integer.toString(this.queueFromAssigner.hashCode());
			this.queueFromAssigner = "FA" + Integer.toString(this.queueFromAssigner.hashCode());

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
			
	}
	/**
	 * This class contains main.
	 * First parameter contains ip address of rabbitmq server 
	 * @param args
	 */	

	public static void main(String[] args) {
		
		Assigner myObject = new Assigner(args[0]);
		System.out.println("connecting on "+myObject.ipAddressRabbitMqServer);
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost(myObject.ipAddressRabbitMqServer);
	    Connection connection = null;
		
	  //send job id to server and initialize queue names
	    
	    try {
			connection = factory.newConnection();
			Channel channel = connection.createChannel();

		    channel.queueDeclare(myObject.initialConnectionQueue, false, false, false, null);
		    channel.basicPublish("", myObject.initialConnectionQueue, null, myObject.jobId.getBytes());
		    System.out.println(myObject.jobId);
		    channel.close();
		    connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    
	    
	    Channel channel = null;
	    try {
			connection = factory.newConnection();
			channel = connection.createChannel();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    
	    
	    for ( int i = 0; i < 10; i++ ) {
		    //send some text to server to test its own queue
	    	try {
				Thread.sleep(10000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				
				
				String message = "Assigner message" + myObject.jobId + i;
			    channel.queueDeclare(myObject.queueFromAssigner, false, false, false, null);
			    channel.basicPublish("", myObject.queueFromAssigner, null, message.getBytes());
			    System.out.println("message sent " + message);

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	    	
	    }
	    try {
			channel.close();
			connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    
	}
}
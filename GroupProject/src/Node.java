import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;


public class Node implements Runnable {
	//implement similar communication mechanism as that of assigner
	//implement 'node polling server' for failure handling
	//keep placeholder for map and reduce functions
	
	String queueFromNode;
	String queueFromServer;
	String nodeId;
	String initialConnectionQueue;
	String ipAddressRabbitMqServer;
	
	class InputtoServer {
		
	}
	
	Node(String ipAddress) {
		//ip address of rabbitmq server
		this.ipAddressRabbitMqServer = ipAddress;
		this.initialConnectionQueue = "nodeQueue";
		//form myQueue as, assigner + hash value of (localhost + current date time)
		InetAddress ip;
		Date currentDate = new Date();
		try {
			ip = InetAddress.getLocalHost();
			this.queueFromNode = ip + currentDate.toString();
			this.nodeId = Integer.toString(this.queueFromNode.hashCode());
			this.queueFromServer = "FS" + Integer.toString(this.queueFromNode.hashCode());
			this.queueFromNode = "FN" + Integer.toString(this.queueFromNode.hashCode());

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
			
	}
	
	public static void main (String[] args) {
		Node myNode = new Node(args[0]);
		//System.out.println("connecting on "+myObject.ipAddressRabbitMqServer);
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost(myNode.ipAddressRabbitMqServer);
	    Connection connection = null;
		
	    //send node id to server and initialize queue names
	    
	    try {
			connection = factory.newConnection();
			Channel channel = connection.createChannel();

		    channel.queueDeclare(myNode.initialConnectionQueue, false, false, false, null);
		    channel.basicPublish("", myNode.initialConnectionQueue, null, myNode.nodeId.getBytes());
		    System.out.println(myNode.nodeId);
		    channel.close();
		    connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    Thread pollServer = new Thread(myNode);
	    pollServer.start();
	    
	    
	    
	}

	/**
	 * This method polls server every minute
	 * If this node goes down, it will stop polling and server will come to know 
	 * that this node has gone down
	 */
	public void pollServer() {
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost(this.ipAddressRabbitMqServer);
		Connection connection = null;		
	    Channel channel = null;
	    
	    try {
			connection = factory.newConnection();
			channel = connection.createChannel();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    
	    
	    while(true) {
		    //send some text to server to test its own queue
	    	try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				String message = "alive";
			    channel.queueDeclare(this.queueFromNode, false, false, false, null);
			    channel.basicPublish("", this.queueFromNode, null, message.getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	    	
	    }
	}
	
	public void listenFromServer() {
		while (true) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("listening from server");
		}
	}
	@Override
	public void run() {
		Thread pollServer = new Thread() {
			public void run() {
				pollServer();
			}
		};
		//continue polling server after each minute
		//runs on a separate thread
		pollServer.start();

		Thread interactWithServer = new Thread() {
			public void run() {
				listenFromServer();
			}
		};
		
		interactWithServer.start();
	}
	
	
}

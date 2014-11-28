import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

import org.python.util.PythonInterpreter;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;


public class Node implements Runnable {
	//implement similar communication mechanism as that of assigner
	//implement 'node polling server' for failure handling
	//keep placeholder for map and reduce functions
	
	String queueFromNode;
	String queueFromServer;
	String nodeId;
	String initialConnectionQueue;
	String ipAddressRabbitMqServer;
	Path mapPath;
	Path reducePath;
	Path inputPath;
	Path mapOutputPath;
	Path firstInputPath;
	Path secondInputPath;
	Path reduceOutputPath;
	
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
		
		mapPath = Paths.get("map.py");
		reducePath = Paths.get("reduce.py");
		inputPath = Paths.get("input.dat");
		firstInputPath = Paths.get("firstInput.dat");
		secondInputPath = Paths.get("secondInput.dat");
		mapOutputPath = Paths.get("output.dat");
		reduceOutputPath = Paths.get("reduceOutput.dat");
			
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
		
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost(this.ipAddressRabbitMqServer);
		Connection connection = null;
		Connection connection1 = null;
		
	    Channel channel = null;
	    Channel channel1 = null;
	    
	    try {
			connection = factory.newConnection();
			channel = connection.createChannel();
			
			connection1 = factory.newConnection();
			channel1 = connection1.createChannel();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    
		try {
			channel.queueDeclare(this.queueFromServer, false, false, false, null);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		QueueingConsumer consumer = new QueueingConsumer(channel);
		try {
			channel.basicConsume(this.queueFromServer, true, consumer);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		while (true) {


			
			try {
				
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				
				String message = new String(delivery.getBody());
				System.out.println(message);
				if(message.equals("map")) {
					FileOutputStream outputFile = new FileOutputStream(this.mapPath.toAbsolutePath().toString());				
					try {
						delivery = consumer.nextDelivery();
						outputFile.write(delivery.getBody());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					outputFile.close();
					
					outputFile = new FileOutputStream(this.inputPath.toAbsolutePath().toString());				
					try {
						delivery = consumer.nextDelivery();
						outputFile.write(delivery.getBody());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					outputFile.close();
					
					//code from Ben's PythonTest
					PythonInterpreter interpreter = new PythonInterpreter();
					
					String mapFunc = "map";
					String inFile = this.inputPath.toString();
					String outFile = this.mapOutputPath.toString();
					
					interpreter.exec("import " + mapFunc);
					interpreter.exec(mapFunc + ".domap(\""+ inFile + "\", \"" + outFile + "\")");
					interpreter.cleanup();
					
					//send output back to server
				    channel1.queueDeclare(this.queueFromNode, false, false, false, null);
				    message = "result";
				    channel1.basicPublish("", this.queueFromNode, null, message.getBytes());
				    channel1.basicPublish("", this.queueFromNode, null, Files.readAllBytes(this.mapOutputPath));
					
				    //cleanup files
				    Files.deleteIfExists(this.mapPath);
				    Files.deleteIfExists(this.mapOutputPath);
				    Files.deleteIfExists(this.inputPath);
				}
				
				else if (message.equals("reduce")) {
					System.out.println("in reduce");
					FileOutputStream outputFile = new FileOutputStream(this.reducePath.toAbsolutePath().toString());				
					try {
						delivery = consumer.nextDelivery();
						outputFile.write(delivery.getBody());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					outputFile.close();
					
					outputFile = new FileOutputStream(this.firstInputPath.toAbsolutePath().toString());				
					try {
						delivery = consumer.nextDelivery();
						outputFile.write(delivery.getBody());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					outputFile.close();
					
					outputFile = new FileOutputStream(this.secondInputPath.toAbsolutePath().toString());				
					try {
						delivery = consumer.nextDelivery();
						outputFile.write(delivery.getBody());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					outputFile.close();
					
					//code from Ben's PythonTest
					PythonInterpreter interpreter = new PythonInterpreter();
					
					String reduceFunc = "reduce";
					String inFile1 = this.firstInputPath.toString();
					String inFile2 = this.secondInputPath.toString();
					String outFile = this.reduceOutputPath.toString();
					
					
					interpreter = new PythonInterpreter();
					interpreter.exec("import " + reduceFunc);
					interpreter.exec(reduceFunc + ".doreduce(\""+ inFile1 + "\", \"" + inFile2 + "\", \"" + outFile + "\")");
					interpreter.cleanup();
					
					//send output back to server
				    channel.queueDeclare(this.queueFromNode, false, false, false, null);
				    message = "result";
				    channel.basicPublish("", this.queueFromNode, null, message.getBytes());
				    channel.basicPublish("", this.queueFromNode, null, Files.readAllBytes(this.reduceOutputPath));
					
				    //cleanup files
				    Files.deleteIfExists(this.reducePath);
				    Files.deleteIfExists(this.firstInputPath);
				    Files.deleteIfExists(this.secondInputPath);
				    Files.deleteIfExists(this.reduceOutputPath);
				}
			//	Path current = Paths.get("");
			//	String outputFileString = current.toAbsolutePath().toString() + "/output.txt";
				

				
			} catch (ShutdownSignalException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ConsumerCancelledException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
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
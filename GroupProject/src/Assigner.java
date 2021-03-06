import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Assigner Class
 * 
 * @author Ben, Bhakti, and Vinayak
 *
 */
public class Assigner {
	
	String queueFromAssigner;
	String queueFromServer;
	String jobId;
	String initialConnectionQueue;
	String ipAddressRabbitMqServer;
	
	String mapPath;
	String reducePath;
	String inputDataPath;
	String outputPath;
	
	/**
	 * 
	 * @param ipAddress
	 * @param map
	 * @param reduce
	 * @param input
	 * @param output
	 */
	Assigner(String ipAddress, String map, String reduce, String input, String output) {
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
			
		this.mapPath = map;
		this.reducePath = reduce;
		this.inputDataPath = input;
		this.outputPath = output + Integer.toString(this.queueFromAssigner.hashCode());
	}
	/**
	 * This class contains main.
	 * First parameter contains ip address of rabbitmq server 
	 * @param args
	 */	

	public static void main(String[] args) {
		
		if (args[0].equals(null) || args[1].equals(null) || args[2].equals(null) || args[3].equals(null) || args[4].equals(null)) {
			System.out.println("Invalid arguments to program. Exiting!");
			System.exit(1);
		}
		Assigner myObject = new Assigner(args[0], args[1], args[2], args[3], args[4]);
		
		System.out.println("creating a new Job with Id : " + myObject.jobId);
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost(myObject.ipAddressRabbitMqServer);
	    Connection connection = null;
		
	    //send job id to server and initialize queue names
	    //make initial communication with server and update jobid
	    //so that server can start communication on individual assigner queues
	    
	    
	    try {
			connection = factory.newConnection();
			Channel channel = connection.createChannel();

		    channel.queueDeclare(myObject.initialConnectionQueue, false, false, false, null);
		    channel.basicPublish("", myObject.initialConnectionQueue, null, myObject.jobId.getBytes());
		    //System.out.println(myObject.jobId);
		    channel.close();
		    connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    //initial communication has been made
	    //assigner and server now communicate on assigner specific queue
	    
	    Channel channel = null;
	    try {
			connection = factory.newConnection();
			channel = connection.createChannel();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	
	    //Write map, reduce scripts and input data to server - publish on queue
	    //and wait for output - consume from queue
  		try {
			
			
  			Path mapFilePath = Paths.get(myObject.mapPath);
  			Path reduceFilePath = Paths.get(myObject.reducePath);
  			Path inputFilePath = Paths.get(myObject.inputDataPath);
  			Path outputFilePath = Paths.get(myObject.outputPath);
  			
		    channel.queueDeclare(myObject.queueFromAssigner, false, false, false, null);
		    
		    System.out.println("Passing Map method to Server");
		    //pass map function
		    String message = "map";
		    channel.basicPublish("", myObject.queueFromAssigner, null, message.getBytes());
		    channel.basicPublish("", myObject.queueFromAssigner, null, Files.readAllBytes(mapFilePath));
		    System.out.println("Map method has been passed to Server");
		    
		    System.out.println("Passing Reduce method to Server");
		    //pass reduce function
		    message = "reduce";
		    channel.basicPublish("", myObject.queueFromAssigner, null, message.getBytes());
		    channel.basicPublish("", myObject.queueFromAssigner, null, Files.readAllBytes(reduceFilePath));
		    System.out.println("Reduce method has been passed to Server");
		    
		    System.out.println("Passing Input Data to Server");
		    //pass input data
		    message = "input";
		    channel.basicPublish("", myObject.queueFromAssigner, null, message.getBytes());
		    channel.basicPublish("", myObject.queueFromAssigner, null, Files.readAllBytes(inputFilePath));
		    System.out.println("Input data has been passed to Server");
		    
		    //consume from server queue
			channel.queueDeclare(myObject.queueFromServer, false, false, false, null);
			
			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(myObject.queueFromServer, true, consumer);
			
			try {
				
				//consume results in the form of output file
				//outputfile will contain either output or error message if any
				
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				
			//	Path current = Paths.get("");
			//	String outputFileString = current.toAbsolutePath().toString() + "/output.txt";
				message = new String(delivery.getBody());
				
				if(message.equals("result")) {
					delivery = consumer.nextDelivery();
					FileOutputStream outputFile = new FileOutputStream(myObject.outputPath);				
					outputFile.write(delivery.getBody());
					outputFile.close();					
				}

				
				System.out.println("Output from Server has been received at " + myObject.outputPath.toString());
				
			} catch (ShutdownSignalException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ConsumerCancelledException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
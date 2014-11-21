import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.ArrayList;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;


public class Server implements Runnable {
	String jobId;
	String queueFromAssigner;
	String queueFromServer;
	boolean jobRequested;
	boolean jobInProgress;
	boolean jobFinished;
	boolean jobFailed;

	Server (String assignerQueue) {
		this.jobId = assignerQueue;
		this.queueFromServer = "FS" + assignerQueue;
		this.queueFromAssigner = "FA" + assignerQueue;
		this.jobRequested = false;
		this.jobFailed = false;
		this.jobFinished = false;
		this.jobInProgress = false;
		System.out.println(this.jobId);
	}
	//just for definition so that Server can deserialize this object
	public static class Assigner {
		
	}
	static class Node {
		String nodeId;
		String queueFromServer;
		String queueFromNode;
		boolean isFree;
		boolean isFailed;
		
		Node(String nodeQueue) {
			this.queueFromNode = "FN" + nodeQueue;
			this.queueFromServer = "FS" + nodeQueue;
			this.isFree = true;
			this.isFailed = false;
		}
	}
	
	static ArrayList<Node> nodeList;
	
	//Following 2 queues are for assigner requests and node requests
	//these 2 components will let Server know about their individual Queues
	static String queueForAssignerRequest = "assignerQueue";
	static String queueForNodeRequest = "nodeQueue";
	static String serverHost;
	/**
	 * deserialize incoming assigner object
	 * @param data
	 * @return
	 */
	public static Assigner deserializeAssignerObject (byte[] data) {
		Assigner temp = null;
		
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInput in;
        
		try {
			in = new ObjectInputStream(bis);
			temp = (Assigner) in.readObject();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
        return temp;
	}
	
	public static void main (String[] args) {
		final ArrayList<Server> jobRequests = new ArrayList<Server>();
		nodeList = new ArrayList<Node>();
		
		//assign rabbitMq Server
		serverHost = args[0];
		
		//Thread to listen requests from Assigner
		//For each request received, a server object will be created
		
		Thread listenAssignerRequests = new Thread () {
			public void run() {
				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost(serverHost);
				Connection connection;
				try {
					connection = factory.newConnection();
					Channel channel = connection.createChannel();					
					channel.queueDeclare(queueForAssignerRequest, false, false, false, null);
					
					QueueingConsumer consumer = new QueueingConsumer(channel);
					channel.basicConsume(queueForAssignerRequest, true, consumer);
					
					while(true) {
						try {
							QueueingConsumer.Delivery delivery = consumer.nextDelivery();
							
							String assignerQueue = new String(delivery.getBody());
							System.out.println("got assignerQueue" + assignerQueue);
							Server newJob = new Server(assignerQueue);
							
							//add new job request to Server
							jobRequests.add(newJob);
							
							Thread threadAssigner = new Thread(newJob);
							threadAssigner.start();
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
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		listenAssignerRequests.start();
		
		//Thread to listen request from nodes
		//Assign queues for nodes
		
		Thread listenNodeRequests = new Thread () {
			public void run() {
				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost(serverHost);
				Connection connection;
				try {
					connection = factory.newConnection();
					Channel channel = connection.createChannel();					
					channel.queueDeclare(queueForNodeRequest, false, false, false, null);
					
					QueueingConsumer consumer = new QueueingConsumer(channel);
					channel.basicConsume(queueForNodeRequest, true, consumer);
					
					while(true) {
						try {
							QueueingConsumer.Delivery delivery = consumer.nextDelivery();
							String nodeQueue = new String(delivery.getBody());
							
							Node newNode = new Node(nodeQueue);
							nodeList.add(newNode);
							
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
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		listenNodeRequests.start();

		
	}

	@Override
	public void run() {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(serverHost);
		Connection connection;
		Channel channel = null;
		
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		

		if(!this.jobRequested) {

			try {

				channel.queueDeclare(this.queueFromAssigner, false, false, false, null);
				
				QueueingConsumer consumer = new QueueingConsumer(channel);
				channel.basicConsume(this.queueFromAssigner, true, consumer);
				
				
				try {
					while(true) {
						QueueingConsumer.Delivery delivery = consumer.nextDelivery();
						
						String message = new String(delivery.getBody());
						System.out.println(message);										
					}

					//output is actually job request from assigner
					//for now it will be considered as string for testing purpose
				} catch (ShutdownSignalException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ConsumerCancelledException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NullPointerException e) {
					System.out.println("no message");
				}
				
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}							
			
		}		
	}
}

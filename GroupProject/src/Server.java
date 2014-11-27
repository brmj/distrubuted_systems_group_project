import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

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
	private static class Assigner {
		
	}
	static class Node implements Runnable {
		String nodeId;
		String queueFromServer;
		String queueFromNode;
		boolean isFree;
		boolean isFailed;
		
		
		Node(String nodeQueue) {
			this.nodeId = nodeQueue;
			this.queueFromNode = "FN" + nodeQueue;
			this.queueFromServer = "FS" + nodeQueue;
			this.isFree = true;
			this.isFailed = false;
		}

		public void listenOnNodeQueue() {
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
			try {

				channel.queueDeclare(this.queueFromNode, false, false, false, null);
				
				QueueingConsumer consumer = new QueueingConsumer(channel);
				channel.basicConsume(this.queueFromNode, true, consumer);
				
				
				try {
					while(true) {
						QueueingConsumer.Delivery delivery = consumer.nextDelivery(5000);
						
						String message = new String(delivery.getBody());
						
						//System.out.println(message);
						
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
					this.isFailed = true;
					System.out.println("node failure");
					Server.deleteNode(this);
				}
				
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}							
		}
		
		@Override
		public void run() {
			//listens on incoming messages from node
			//including polling messages and output messages
			
			this.listenOnNodeQueue();
		}
		
		
	}
	
	static ArrayList<Node> nodeList;
	
	//Following 2 queues are for assigner requests and node requests
	//these 2 components will let Server know about their individual Queues
	static String queueForAssignerRequest = "assignerQueue";
	static String queueForNodeRequest = "nodeQueue";
	static String serverHost;
	
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
							System.out.println("created a new node " + nodeQueue);
							Thread listenOnNode = new Thread(newNode);
							listenOnNode.start();
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
	
	public void getMapReduceInput(Connection connection, Channel channel) {
		try {
			String message = "";
			
			channel.queueDeclare(this.queueFromAssigner, false, false, false, null);
			
			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(this.queueFromAssigner, true, consumer);
			
			
			try {

				//first read map keyword
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();					
				message = new String(delivery.getBody());
				
				//write map function with job id
				if(message.equals("map")) {
					delivery = consumer.nextDelivery();
					Path mapPath = Paths.get("map" + this.jobId);
					FileOutputStream mapFileOutput = new FileOutputStream(mapPath.toAbsolutePath().toString());
					mapFileOutput.write(delivery.getBody());
					mapFileOutput.close();						
				}
				
				//for reduce function
				delivery = consumer.nextDelivery();					
				message = new String(delivery.getBody());
				
				//write reduce function with job id
				if(message.equals("reduce")) {
					delivery = consumer.nextDelivery();
					Path reducePath = Paths.get("reduce" + this.jobId);
					FileOutputStream reduceFileOutput = new FileOutputStream(reducePath.toAbsolutePath().toString());
					reduceFileOutput.write(delivery.getBody());
					reduceFileOutput.close();						
				}
				
				//for reduce function
				delivery = consumer.nextDelivery();					
				message = new String(delivery.getBody());
				
				//write reduce function with job id
				if(message.equals("input")) {
					delivery = consumer.nextDelivery();
					Path inputPath = Paths.get("input" + this.jobId);
					FileOutputStream inputFileOutput = new FileOutputStream(inputPath.toAbsolutePath().toString());
					inputFileOutput.write(delivery.getBody());
					inputFileOutput.close();						
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
	
	public void distributeJob(Connection connection, Channel channel) {
		synchronized(Server.nodeList) {
			int numberOfNodes = 0;
			
			for (int count=0; count < nodeList.size(); count++) {
				if(nodeList.get(count).isFree) {
					numberOfNodes++;
				}
				
			}
			
			System.out.println("number of nodes " + numberOfNodes);
			
			List<String> linesInputFile = null;
			try {
				linesInputFile = Files.readAllLines(Paths.get("input" + this.jobId), Charset.defaultCharset());				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			int numberOfLines = linesInputFile.size();			
			int linesPerNode = numberOfLines / numberOfNodes;
			int linesCounter = 0;
			int nodeCounter = 0;
			
			System.out.println("number of lines " + numberOfLines + " linesPerNode " + linesPerNode);
			FileInputStream readInputFile = null;
			BufferedReader bufferReader = null;
			String inputLine = null;
			try {
				readInputFile = new FileInputStream(Paths.get("input" + this.jobId).toAbsolutePath().toString());
				bufferReader = new BufferedReader(new InputStreamReader(readInputFile));
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//create input files for nodes
			
			for (int count=0; count < nodeList.size(); count++) {
				if(nodeList.get(count).isFree) {
					if ( nodeCounter == (numberOfNodes - 1)) {
						//add all remaining lines to last available node
						
						try {
							PrintWriter outputLine = new PrintWriter(Paths.get("inputnode" + nodeList.get(count).nodeId).toAbsolutePath().toString() );
							
							while((inputLine = bufferReader.readLine()) != null ) {
								outputLine.println(inputLine);
							}
							
							outputLine.close();
							
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}
					
					else {
						try {
							
							PrintWriter outputLine = new PrintWriter(Paths.get("inputnode" + nodeList.get(count).nodeId).toAbsolutePath().toString() );
							while (linesCounter < linesPerNode) {									
								if ( (inputLine = bufferReader.readLine()) != null ) {
								
									outputLine.println(inputLine);
									linesCounter++;
								}
								
							}
							outputLine.close();
							linesCounter = 0;
							nodeCounter++;
							
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}						
					}
				}
			}
		}
	}
	
	@Override
	public void run() {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(serverHost);
		Connection connection = null;
		Channel channel = null;
		
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		getMapReduceInput(connection, channel);
		distributeJob(connection, channel);
			
	}
	
	public static void deleteNode(Node thisNode) {
		Server.nodeList.remove(thisNode);
		System.out.println("node deleted");
	}
}
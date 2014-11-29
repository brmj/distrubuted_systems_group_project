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

/**
 * 
 * @author Ben, Bhakti, and Vinayak
 *
 */
public class Server implements Runnable {
	String jobId;
	String queueFromAssigner;
	String queueFromServer;
	boolean jobRequested;
	boolean jobInProgress;
	boolean jobFinished;
	boolean jobFailed;

	/**
	 * 
	 * @param assignerQueue
	 */
	Server (String assignerQueue) {
		this.jobId = assignerQueue;
		this.queueFromServer = "FS" + assignerQueue;
		this.queueFromAssigner = "FA" + assignerQueue;
		this.jobRequested = false;
		this.jobFailed = false;
		this.jobFinished = false;
		this.jobInProgress = false;
		//System.out.println(this.jobId);
	}
	
	/**
	 * Stores incoming results from nodes
	 * @author Ben, Bhakti, and Vinayak
	 *
	 */
	static class Result {
		String jobId;
		byte[] result;
		
		public Result(String id, byte[] data) {
			this.jobId = id;
			this.result = data;
		}
	}
	
	static ArrayList<Result> resultsList = new ArrayList<Result>();
	
	/**
	 * 
	 * @author Ben, Bhakti, and Vinayak
	 *
	 */
	static class Node implements Runnable {
		String jobId;
		String nodeId;
		String queueFromServer;
		String queueFromNode;
		boolean isFree;
		boolean isFailed;
		
		/**
		 * 
		 * @param nodeQueue
		 */
		Node(String nodeQueue) {
			this.nodeId = nodeQueue;
			this.queueFromNode = "FN" + nodeQueue;
			this.queueFromServer = "FS" + nodeQueue;
			this.isFree = true;
			this.isFailed = false;
			this.jobId = "none";
		}
		/**
		 * 
		 */
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
						
						//if message is other than alive 
						//start writing message in queue so that it can be retrieved
						
						//System.out.println(message);
						
						if(message.equals("result")) {	
							
							System.out.println("Partial result has been received for Job Id : " + this.jobId );
							
							while (true) {
								delivery = consumer.nextDelivery(60000);
								message = new String(delivery.getBody());
								if(!message.equals("alive")) {
									break;
								}
							}
							
							
							synchronized(resultsList) {
								Result result = new Result(this.jobId, message.getBytes());
								resultsList.add(result);	
								//System.out.println("size " + resultsList.size());
							}
							this.jobId = "none";
							this.isFree = true;

						}
						
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
					System.out.println("node failure has occured for Node Id : " + this.nodeId);
					Server.deleteNode(this);
				}
				
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}							
		}
		/**
		 * 
		 */
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
	/**
	 * 
	 * @param args
	 */
	public static void main (String[] args) {
		final ArrayList<Server> jobRequests = new ArrayList<Server>();
		nodeList = new ArrayList<Node>();
		
		//assign rabbitMq Server
		if(args[0].equals(null)) {
			System.exit(1);
		}
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
							System.out.println("New Job Request with Job Id : " + assignerQueue);
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
							System.out.println("New Node has joined the system with Node Id : " + nodeQueue);
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
	/**
	 * get map reduce and input file from assigner
	 * @param connection
	 * @param channel
	 */
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
					System.out.println("Receiving Map Method for Job Id : " + this.jobId);
					delivery = consumer.nextDelivery();
					Path mapPath = Paths.get("map" + this.jobId);
					FileOutputStream mapFileOutput = new FileOutputStream(mapPath.toAbsolutePath().toString());
					mapFileOutput.write(delivery.getBody());
					mapFileOutput.close();						
					System.out.println("Map method has been received for Job Id : " + this.jobId);
				}
				
				//for reduce function
				delivery = consumer.nextDelivery();					
				message = new String(delivery.getBody());
				
				//write reduce function with job id
				if(message.equals("reduce")) {
					System.out.println("Receiving Reduce Method for Job Id : " + this.jobId);
					delivery = consumer.nextDelivery();
					Path reducePath = Paths.get("reduce" + this.jobId);
					FileOutputStream reduceFileOutput = new FileOutputStream(reducePath.toAbsolutePath().toString());
					reduceFileOutput.write(delivery.getBody());
					reduceFileOutput.close();						
					System.out.println("Reduce method has been received for Job Id : " + this.jobId);
				}
				
				//for reduce function
				delivery = consumer.nextDelivery();					
				message = new String(delivery.getBody());
				
				//write reduce function with job id
				if(message.equals("input")) {
					System.out.println("Receiving input data for Job Id : " + this.jobId);
					delivery = consumer.nextDelivery();
					Path inputPath = Paths.get("input" + this.jobId);
					FileOutputStream inputFileOutput = new FileOutputStream(inputPath.toAbsolutePath().toString());
					inputFileOutput.write(delivery.getBody());
					inputFileOutput.close();				
					System.out.println("Input data has been received for Job Id : " + this.jobId);
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
				//System.out.println("no message");
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}									
	}
	/**
	 * 
	 * @param connection
	 * @param channel
	 */
	public void distributeJob(Connection connection, Channel channel) {
		synchronized(Server.nodeList) {
			int numberOfNodes = 0;
			
			while(numberOfNodes < 1) {
				for (int count=0; count < nodeList.size(); count++) {
					if(nodeList.get(count).isFree) {
						numberOfNodes++;
					}
					
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
			//System.out.println("number of nodes " + numberOfNodes);
			
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
			
		//	System.out.println("number of lines " + numberOfLines + " linesPerNode " + linesPerNode);
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
			System.out.println("Splitting & distribution of input data begins for Job Id : " + this.jobId);
			
			for (int count=0; count < nodeList.size(); count++) {
				if(nodeList.get(count).isFree) {
					if ( nodeCounter == (numberOfNodes - 1)) {
						//add all remaining lines to last available node
						
						try {
							PrintWriter outputLine = new PrintWriter(Paths.get("inputnode"  + this.jobId + nodeList.get(count).nodeId).toAbsolutePath().toString() );
							
							while((inputLine = bufferReader.readLine()) != null ) {
								outputLine.println(inputLine);
							}
							
							outputLine.close();
							
							
							//send map function and input data
							
							nodeList.get(count).isFree = false;
							nodeList.get(count).jobId = this.jobId;
							
							String message = "map";
						    channel.queueDeclare(nodeList.get(count).queueFromServer, false, false, false, null);
						    channel.basicPublish("", nodeList.get(count).queueFromServer, null, message.getBytes());
						   // System.out.println(Paths.get("map" + this.jobId).toAbsolutePath());
						   // System.out.println(Paths.get("inputnode" + nodeList.get(count).nodeId).toAbsolutePath());
						    channel.basicPublish("", nodeList.get(count).queueFromServer, null, Files.readAllBytes(Paths.get("map" + this.jobId).toAbsolutePath()));						    
						    channel.basicPublish("", nodeList.get(count).queueFromServer, null, Files.readAllBytes(Paths.get("inputnode" + this.jobId + nodeList.get(count).nodeId).toAbsolutePath()));
						    System.out.println("Node id : " + nodeList.get(count).nodeId + " has been assigned Map Method and split input data for Job Id : " + this.jobId);
							Files.deleteIfExists(Paths.get("inputnode" + this.jobId + nodeList.get(count).nodeId));
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
							
							PrintWriter outputLine = new PrintWriter(Paths.get("inputnode" + this.jobId + nodeList.get(count).nodeId).toAbsolutePath().toString() );
							while (linesCounter < linesPerNode) {									
								if ( (inputLine = bufferReader.readLine()) != null ) {
								
									outputLine.println(inputLine);
									linesCounter++;
								}
								
							}
							outputLine.close();
							linesCounter = 0;
							nodeCounter++;
							
							//send map function and input 
							nodeList.get(count).isFree = false;
							
						    nodeList.get(count).jobId = this.jobId;
							
							String message = "map";
						    channel.queueDeclare(nodeList.get(count).queueFromServer, false, false, false, null);
						    channel.basicPublish("", nodeList.get(count).queueFromServer, null, message.getBytes());
						    channel.basicPublish("", nodeList.get(count).queueFromServer, null, Files.readAllBytes(Paths.get("map" + this.jobId).toAbsolutePath()));
						    channel.basicPublish("", nodeList.get(count).queueFromServer, null, Files.readAllBytes(Paths.get("inputnode" + this.jobId + nodeList.get(count).nodeId).toAbsolutePath()));							
						    System.out.println("Node id : " + nodeList.get(count).nodeId + " has been assigned Map Method and split input data for Job Id : " + this.jobId);
						    Files.deleteIfExists(Paths.get("inputnode" + this.jobId + nodeList.get(count).nodeId));
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}						
					}
				}
			}
		}
	}
	/**
	 * 
	 * @param connection
	 * @param channel
	 */
	public void reduceOutput( Connection connection, Channel channel ) {
		while(true) {
			int counterNode = 0;
			int counterFreeNode = 0;
			
			for(int i = 0; i < nodeList.size(); i++ ) {
				if(nodeList.get(i).jobId.equals(this.jobId)) {
					counterNode++;	
				}
				if(nodeList.get(i).isFree) {
					counterFreeNode++;
				}
			}
			
			int counterResult = 0;
			for(int i = 0; i < resultsList.size(); i++ ) {
				if(resultsList.get(i).jobId.equals(this.jobId)) {
					counterResult++;
				}
			}
			
			if (counterNode == 0 && counterResult == 1) {
				String message = "result";
			    try {
					channel.queueDeclare(this.queueFromServer, false, false, false, null);
				    channel.basicPublish("", this.queueFromServer, null, message.getBytes());
				    channel.basicPublish("", this.queueFromServer, null, resultsList.get(0).result);
				    //System.out.println("here");
				    resultsList.remove(0);
				    System.out.println("Job has been completed and output file has been sent to Assigner for Job Id : " + this.jobId);
				    Files.deleteIfExists(Paths.get("map" + this.jobId));
				    Files.deleteIfExists(Paths.get("reduce" + this.jobId));
				    Files.deleteIfExists(Paths.get("input" + this.jobId));
				    break;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			else {
				if (counterFreeNode > 0 && counterResult > 1) {
					//System.out.println("coming for reduce");
					Result firstResult = resultsList.get(0);
					Result secondResult = resultsList.get(1);
					
					for(int i = 0; i < nodeList.size(); i++ ) {
						if (nodeList.get(i).isFree) {
							String message = "reduce";
						    try {
								channel.queueDeclare(nodeList.get(i).queueFromServer, false, false, false, null);
							    channel.basicPublish("", nodeList.get(i).queueFromServer, null, message.getBytes());
							    channel.basicPublish("", nodeList.get(i).queueFromServer, null, Files.readAllBytes(Paths.get("reduce" + this.jobId).toAbsolutePath()));
							    channel.basicPublish("", nodeList.get(i).queueFromServer, null, firstResult.result);
							    channel.basicPublish("", nodeList.get(i).queueFromServer, null, secondResult.result);
							    nodeList.get(i).isFree = false;
							    nodeList.get(i).jobId = this.jobId;
							    System.out.println("Node id : " + nodeList.get(i).nodeId + " has been assigned Reduce Method and 2 input data for Job Id : " + this.jobId);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						    break;
						}
					}
					resultsList.remove(firstResult);
					resultsList.remove(secondResult);
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
		reduceOutput(connection, channel);
	}
	
	public static void deleteNode(Node thisNode) {
		System.out.println("Removing node with Node Id : " + thisNode.nodeId);
		Server.nodeList.remove(thisNode);
	}
}

public class Assigner {
	
		
	/**
	 * This class contains main. 
	 * 1. End user needs to write a separate class which implements
	 * IAssigner interface.
	 * 2. Create a jar file containing that class. (this may be done by
	 * main method here - need to investigate.)
	 * 3. Execute Assigner java program and provide
	 * command line arguments - classname.jar data.txt
	 * 4. main method will send this data to Server for processing.
	 * 5. main method will receive results, error messages, logs.
	 * 6. program will exit after it has received response from server. 
	 * 7. Multithreading wont be required as Assigner will run as an individual instance
	 * each time mapper and reducer functions are sent to server
	 * 
	 * @param args
	 */	
	
	public static void main(String[] args) {
		//validate jar file and text file
		//send these 2 components to Server
		//wait for responses
	}
}

import java.io.*;
import java.util.ArrayList;

import org.python.core.PyObject;
import org.python.core.PyInteger;
import org.python.core.PyException;
import org.python.util.PythonInterpreter;


public class PythonTest {

	
	public static void doMap(String mapFunc, String inFile, String outFile){
		PythonInterpreter interpreter = new PythonInterpreter();
		interpreter.exec("import " + mapFunc);
		interpreter.exec(mapFunc + ".domap(\""+ inFile + "\", \"" + outFile + "\")");
		interpreter.cleanup();
	}
	
	public static void doReduce(String reduceFunc, String inFile1, String inFile2, String outFile){
		PythonInterpreter interpreter = new PythonInterpreter();
		interpreter.exec("import " + reduceFunc);
		interpreter.exec(reduceFunc + ".doreduce(\""+ inFile1 + "\", \"" + inFile2 + "\", \"" + outFile + "\")");
		interpreter.cleanup();
	}
	
	public static void genTestFile(String mapFunc, String filename, ArrayList<Integer> inList){
		PythonInterpreter interpreter = new PythonInterpreter();
		interpreter.exec("import " + mapFunc);
		interpreter.set("l", inList);
		interpreter.exec(mapFunc + ".listtofile(\""+filename+ "\", l)");
		interpreter.cleanup();
	}
	
	public static void getFromFile(String reduceFunc, String filename){
		PythonInterpreter interpreter = new PythonInterpreter();
		interpreter.exec("import " + reduceFunc);
		interpreter.exec("l = " + reduceFunc + ".filetolist(\""+filename+ "\")");
		interpreter.exec("print l");
		interpreter.cleanup();

	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		ArrayList<Integer> i1 = new ArrayList<Integer>();
		ArrayList<Integer> i2 = new ArrayList<Integer>();

		i1.add(5);
		i1.add(3);
		i1.add(9);
		i1.add(3);
		i1.add(1);
		
		i2.add(1);
		i2.add(5);
		i2.add(7);
		i2.add(2);
		
		//genTestFile("sortmap", "i1.dat", i1);
	//	genTestFile("sortmap", "i2.dat", i2);
		
		doMap("sortmap", "i1.dat", "o1.dat");
		doMap("sortmap", "i2.dat", "o2.dat");

		doReduce("sortreduce", "o1.dat", "o2.dat", "o3.dat");
		
		getFromFile("sortreduce", "o3.dat");
		
		

		
	}
	
	
}

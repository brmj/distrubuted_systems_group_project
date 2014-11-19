import java.io.*;
import org.python.core.PyObject;
import org.python.core.PyInteger;
import org.python.core.PyException;
import org.python.util.PythonInterpreter;


public class PythonTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String s = null;
		
		PythonInterpreter interpreter = new PythonInterpreter();
		interpreter.set("foo", "Hello World");
		interpreter.exec("print foo");
	}

}

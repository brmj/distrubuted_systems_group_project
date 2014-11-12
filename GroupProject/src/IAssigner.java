
public interface IAssigner {
	//map function will perform operation on subset of data
	public void map();
	
	//reduce function will combine 2 or more subsets and will return data back to the server
	public void reduce();
}

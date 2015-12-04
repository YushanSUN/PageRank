package pagerank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

/*
 * VERY IMPORTANT 
 * 
 * Each time you need to read/write a file, retrieve the directory path with conf.get 
 * The paths will change during the release tests, so be very carefully, never write the actual path "data/..." 
 * CORRECT:
 * String initialVector = conf.get("initialRankVectorPath");
 * BufferedWriter output = new BufferedWriter(new FileWriter(initialVector + "/vector.txt"));
 * 
 * WRONG
 * BufferedWriter output = new BufferedWriter(new FileWriter(data/initialVector/vector.txt"));
 */

public class PageRank {
	

	public static void createInitialRankVector(String directoryPath, long n) throws IOException  
	{

		File dir = new File(directoryPath);
		if(! dir.exists())
			FileUtils.forceMkdir(dir);
		BufferedWriter output = new BufferedWriter(new FileWriter(dir + "/part-r-00000"));
		double valu = 1.0/n;
		for(int i=1;i<n+1;i++){
			//System.out.println(1);
			output.write(i+"\t"+String.valueOf(valu));
			//System.out.println(valu);
			output.newLine();
		}
		output.close();
		//TO DO			
	}
	
	public static boolean checkConvergence(String initialDirPath, String iterationDirPath, double epsilon) throws IOException
	{
		//TO DO
		// you need to use the L1 norm 
		double value = 0;
		File dir1 = new File(initialDirPath);
		File dir2 = new File(iterationDirPath);
		BufferedReader ini = new BufferedReader(new FileReader(dir1 + "/part-r-00000"));
		BufferedReader ite = new BufferedReader(new FileReader(dir2 + "/part-r-00000"));
		String valueString = null;
		while ((valueString=ini.readLine())!=null){
			double v1 = Double.parseDouble(valueString.split("\t")[1]);
			double v2 = Double.parseDouble(ite.readLine().split("\t")[1]);
			value += Math.abs(v2-v1);
			System.out.println(value);
		}
		ini.close();
		ite.close();
		
		if(value<epsilon){
			return true;
		}
		else{
			return false;	
		}
	}
	
	public static void avoidSpiderTraps(String vectorDirPath, long nNodes, double beta) throws IOException 
	{
		//TO DO
		BufferedReader in = new BufferedReader(new FileReader(vectorDirPath + "/part-r-00000"));
		ArrayList<Double> val = new ArrayList<Double>();
		double v;
		for(int i=1;i<nNodes+1;i++){
			v = Double.parseDouble(in.readLine().split("\t")[1]);
			v = v*beta + ((1-beta)/nNodes);
			val.add(v);
		}
		in.close();
		
		BufferedWriter out = new BufferedWriter(new FileWriter(vectorDirPath + "/part-r-00000"));
		for(int i=1;i<nNodes+1;i++){
			out.write(i+"\t"+val.get(i-1));
			out.newLine();
		}
		out.close();
		
		
	}
	
	public static void iterativePageRank(Configuration conf) 
			throws IOException, InterruptedException, ClassNotFoundException
	{
		
		
		String initialVector = conf.get("initialVectorPath");
		String currentVector = conf.get("currentVectorPath");
		
		String finalVector = conf.get("finalVectorPath");		
		/*here the testing system will search for the final rank vector*/
		
		Double epsilon = conf.getDouble("epsilon", 0.1);
		Double beta = conf.getDouble("beta", 0.8);

 
		//TO DO
		Long nNodes = conf.getLong("numNodes", 0);
		//System.out.println(nNodes);
		createInitialRankVector(initialVector,nNodes);
		GraphToMatrix.job(conf);
		
		
		boolean conver = false;
		while(!conver){
			MatrixVectorMult.job(conf);
			avoidSpiderTraps(currentVector, nNodes, beta);
			
			conver = checkConvergence(initialVector, currentVector, epsilon); 
			if(conver){
				//FileUtils.deleteQuietly(new File(conf.get("initialVectorPath")));
				FileUtils.copyDirectory(new File(currentVector), new File(finalVector));
				//FileUtils.deleteQuietly(new File(conf.get("currentVectorPath")));
				//conver = true;
			}
			else{
			/*	FileUtils.deleteQuietly(new File(initialVector));
				FileUtils.copyDirectory(new File(currentVector), new File(initialVector));
				FileUtils.deleteQuietly(new File(currentVector));
			*/	
				BufferedReader ini = new BufferedReader(new FileReader(currentVector + "/part-r-00000"));
				BufferedWriter ite = new BufferedWriter(new FileWriter(initialVector + "/part-r-00000"));
				for(int i=0;i<nNodes;i++){
					ite.write(ini.readLine());
					ite.write("\n");
				}
				ite.close();
				ini.close();
				FileUtils.deleteDirectory(new File(currentVector));
				
				
			}
		}
		// to retrieve the number of nodes use long nNodes = conf.getLong("numNodes", 0); 

		

		// when you finished implementing delete this line
		//throw new UnsupportedOperationException("Implementation missing");
		
	}
}

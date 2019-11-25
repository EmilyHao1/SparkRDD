package project3;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class CreateDatasetP {

	public static void main(String[] args) {
		
		try {
			FileOutputStream out = new FileOutputStream("datasetPointsforP3.txt" );
			Random rand = new Random();
			int linesLens = 12000000;
		    int x = 0;
		    int y = 0;
			for(int i = 1; i <= linesLens; i++) {
		    	
		    	// x axis of points
				x = 1 + rand.nextInt(10000);
				
				// y axis of points
				y = 1 + rand.nextInt(10000);
				
				// write the x and y axis 
				out.write((x+","+y).getBytes());
				
		    	// switch to the next line
		    	out.write("\r\n".getBytes());
		    	
		    }
		    out.close();
		}catch(IOException e) {
			System.out.print("Exception");
		}

	}

}

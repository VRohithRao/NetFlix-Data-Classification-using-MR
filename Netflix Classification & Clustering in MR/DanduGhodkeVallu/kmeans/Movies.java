/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package movies;
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Random;
 

/**
 *
 * @author BharatRaju
 */
public class Movies {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        
        try{
            File filename = new File("movies.txt");
            FileWriter fileWritter = new FileWriter(filename.getName(),true);
    	    BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            int i=0;
            int length = 1000000;
            Random randomYear = new Random();
            Random randomRating = new Random();
            String movie;
            int year;
            Double rating;
            for(i=0;i<90000000;i++){
                year = randomYear.nextInt((2007-1996)+ 1) + 1996;
                rating = 1.0 + (5.0-1.0) * randomRating.nextDouble();
                movie = "Movie" + Integer.toString(i) + '\t' + 
                        Integer.toString(year) + '\t' + Double.toString(rating);
                bufferWritter.write(movie);
                bufferWritter.newLine();
            }
    	    
    	    bufferWritter.close();
 
	    System.out.println("Done");
        }
        catch(Exception e){
            System.out.println(e);
        }
        
        
    }
    
}

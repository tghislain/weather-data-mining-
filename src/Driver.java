import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Driver {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

		// ask questions
		System.out.println("Please enter the starting year");
		Scanner in = new Scanner(System.in);
		int starYear = in.nextInt();

		System.out.println("Please enter the ending year");
		int endYear = in.nextInt();

		System.out.println("Please enter the starting month");
		int startMonth = in.nextInt();

		System.out.println("Please enter the ending month");
		int endMonth = in.nextInt();

		System.out.println("Type Max for maximum or Min for Minimum");
		String element = in.next();
		System.out.println(element);
		in.close();

		Path currentRelativePath = Paths.get("");
		String s = currentRelativePath.toAbsolutePath().toString();
		//File folder = new File(s+"/Data");
		File folder = new File(s);
		File[] listOfFiles = folder.listFiles();
		ConcurrentLinkedQueue<String> Stations = new ConcurrentLinkedQueue<String>();

		// add stations to a concurrent queue
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				if (listOfFiles[i].getName().contains("USC"))
					;
				Stations.add(listOfFiles[i].getName());
			} else if (listOfFiles[i].isDirectory()) {

			}
		}

		// get all stations
		//File stations = new File("Data/ghcnd-stations.txt");
		File stations = new File("ghcnd-stations.txt");
		BufferedReader br = new BufferedReader(new FileReader(stations));
		String thisLine = br.readLine();
		List<StationData> stationData = new ArrayList<StationData>();

		while (thisLine != null) {
			StationData sd = new StationData();

			sd.id = thisLine.substring(0, 11);
			sd.latitude = Float.valueOf(thisLine.substring(12, 20).trim());
			sd.longitude = Float.valueOf(thisLine.substring(21, 30).trim());
			sd.elevation = Float.valueOf(thisLine.substring(31, 37).trim());
			sd.state = thisLine.substring(38, 40);
			sd.name = thisLine.substring(41, 71);
			if (sd.id.contains("USC"))
				stationData.add(sd);
 
			thisLine = br.readLine();
		}
		 	 

		// Get ExecutorService from Executors utility class
		ExecutorService executor = Executors.newFixedThreadPool(20);

		// create a list to hold the Future object associated with Callable
		List<Future<Object>> list = new ArrayList<Future<Object>>();
		// create a list that will contain the results from the first set of
		// futures
		ConcurrentLinkedQueue<ArrayList<WeatherData>> firstResults = new ConcurrentLinkedQueue<ArrayList<WeatherData>>();
		// create a second list that will contain the results from the last 4
		// futures
		ArrayList<ArrayList<WeatherData>> SecondResults = new ArrayList<ArrayList<WeatherData>>();

		// Create MyCallable instances
		// pass stations files to each callable instance
		while (Stations.size() > 0) {

			//String filename = "Data/" + Stations.poll();
			String filename = Stations.poll();
			Worker callable = new Worker(starYear, endYear, startMonth, endMonth, element.toUpperCase(), filename);
			Future<Object> future = executor.submit(callable);
			// add Future to the list, we can get return value using Future
			list.add(future);
		}
		// get the first results
		for (Future<Object> fut : list) {
			try {

				ArrayList<WeatherData> temp = (ArrayList<WeatherData>) fut.get();
				firstResults.add(temp);

			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}

		}

		// create separate input lists for the last four callables
		ArrayList<ArrayList<WeatherData>> one = new ArrayList<ArrayList<WeatherData>>();
		ArrayList<ArrayList<WeatherData>> two = new ArrayList<ArrayList<WeatherData>>();
		ArrayList<ArrayList<WeatherData>> three = new ArrayList<ArrayList<WeatherData>>();
		ArrayList<ArrayList<WeatherData>> four = new ArrayList<ArrayList<WeatherData>>();

		// divide input for the last four futures
		while (firstResults.size() > 0) {
			int size = firstResults.size();
			one.add(firstResults.poll());
			two.add(firstResults.poll());
			three.add(firstResults.poll());
			four.add(firstResults.poll());

		}

		// call second worker to get the four futures
		Worker2 callable1 = new Worker2(one);
		Future<Object> future1 = executor.submit(callable1);
		Worker2 callable2 = new Worker2(two);
		Future<Object> future2 = executor.submit(callable2);
		Worker2 callable3 = new Worker2(three);
		Future<Object> future3 = executor.submit(callable3);
		Worker2 callable4 = new Worker2(four);
		Future<Object> future4 = executor.submit(callable4);

		// get results from 4 futures

		try {

			ArrayList<WeatherData> temp1 = (ArrayList<WeatherData>) future1.get();
			ArrayList<WeatherData> temp2 = (ArrayList<WeatherData>) future2.get();
			ArrayList<WeatherData> temp3 = (ArrayList<WeatherData>) future3.get();
			ArrayList<WeatherData> temp4 = (ArrayList<WeatherData>) future4.get();
			SecondResults.add(temp1);
			SecondResults.add(temp2);
			SecondResults.add(temp3);
			SecondResults.add(temp4);
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}

		// shut down the executor service now
		executor.shutdown();

		// this list will contain the final results
		ArrayList<WeatherData> thefive = new ArrayList<WeatherData>();

		// process results from the last results to get the final five results
		for (int i = 0; i < SecondResults.size(); i++) {
			if (i == 0)
				thefive = (SecondResults.get(i));
			else {
				for (int m = 0; m < SecondResults.get(i).size(); m++) {
					WeatherData wd = SecondResults.get(i).get(m);
					if (element.toUpperCase().equals("MIN")) {
						for (int n = 0; n < thefive.size(); n++) {
							if (wd.value < thefive.get(n).value) {
								thefive.remove(n);
								thefive.add(wd);
								break;
							}
						}
					} else {
						for (int n = 0; n < thefive.size(); n++) {
							if (wd.value > thefive.get(n).value) {
								thefive.remove(n);
								thefive.add(wd);
								break;
							}
						}
					}
				}

			}
		}

		// print the results
		for (WeatherData wd : thefive)
			if (wd != null) {
				WeatherData w = (WeatherData) wd;
				System.out.print("id = " + w.id + " ");
				System.out.print("year = " + w.year + " ");
				System.out.print("month = " + w.month + " ");
				System.out.print("day = " + w.day + " ");
				System.out.print("qflag = " + w.qflag + " ");
				System.out.println("value = " + (float)(w.value /10.5) + " ");
				for(int i = 0; i < stationData.size(); i++){
					if(stationData.get(i).id.equals(wd.id)){
						System.out.print("id = "+stationData.get(i).id);
						System.out.print(" longitude = "+stationData.get(i).latitude);
						System.out.print(" latitude = "+stationData.get(i).latitude);
						System.out.print(" state = "+stationData.get(i).state);
						System.out.println(" name = "+stationData.get(i).name);
					}
				}
			}
		System.out.println();
	}

}

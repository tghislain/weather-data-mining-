import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.concurrent.Callable;

//this class will process the station files
public class Worker implements Callable<Object> {

	int startYear;
	int endYear;
	int startMonth;
	int endMonth;
	String element;
	String filename;

	public Worker(int startYear, int endYear, int startMonth, int endMonth, String element, String filename) {
		this.startYear = startYear;
		this.endYear = endYear;
		this.startMonth = startMonth;
		this.endMonth = endMonth;
		this.element = element;
		this.filename = filename;
	}

	@Override
	public Object call() throws Exception {
		// read line by line from this station
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String thisLine = br.readLine();
		ArrayList<WeatherData> thefive = new ArrayList<WeatherData>();

		if (filename.contains("USC"))
			while (thisLine != null) {
				String id = thisLine.substring(0, 11);
				int year = Integer.valueOf(thisLine.substring(11, 15).trim());
				int month = Integer.valueOf(thisLine.substring(15, 17).trim());
				String element2 = thisLine.substring(17, 21);
				int days = (thisLine.length() - 21) / 8;
				if ((year >= startYear && year <= endYear)) {

					if ((month >= startMonth && month <= endMonth)) {

						for (int i = 0; i < days; i++) { // Process each day
															// in
							// the line.
							WeatherData wd = new WeatherData();
							wd.day = i + 1;
							int value = Integer.valueOf(thisLine.substring(21 + 8 * i, 26 + 8 * i).trim());
							String qflag = thisLine.substring(27 + 8 * i, 28 + 8 * i);
							wd.id = id;
							wd.year = year;
							wd.month = month;
							wd.element = element2;
							wd.value = value;
							wd.qflag = qflag;

							if (value != -9999 && qflag.equals(" ")) {
								if (thefive.size() < 5 && (wd.element.equals("TMIN") || wd.element.equals("TMAX")))
									thefive.add(wd);
								else {
									for (int x = 0; x < thefive.size(); x++) {
										if (value < thefive.get(x).value && element.toUpperCase().equals("MIN")
												&& wd.element.equals("TMIN")) {
											thefive.remove(x);
											thefive.add(wd);
											break;
										} else if (value > thefive.get(x).value && element.toUpperCase().equals("MAX")
												&& wd.element.equals("TMAX")) {
											thefive.remove(x);
											thefive.add(wd);
											break;
										}
									}
								}
							}
						}
					}

				}

				thisLine = br.readLine();

			}

		br.close();

		ArrayList<WeatherData> results = thefive;

		return results;
	}
}

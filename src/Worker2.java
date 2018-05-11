import java.util.ArrayList;
import java.util.concurrent.Callable;

//this class will process the results from the first set of futures
public class Worker2 implements Callable<Object> {
	ArrayList<ArrayList<WeatherData>> input;

	public Worker2(ArrayList<ArrayList<WeatherData>> in) {
		this.input = in;
	}

	@Override
	public Object call() throws Exception {
		// process own portion on the second results
		ArrayList<WeatherData> thefive = new ArrayList<WeatherData>();
		for (int i = 0; i < input.size(); i++) {
			if (i == 0)
				thefive = (input.get(i));
			else {
				for (int m = 0; m < input.get(i).size(); m++) {
					WeatherData wd = input.get(i).get(m);
					if (wd != null)
						if (wd.element.toUpperCase().equals("TMIN")) {
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
		return thefive;
	}
}

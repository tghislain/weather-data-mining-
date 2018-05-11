
class StationData {
	String id;
	float latitude;
	float longitude;
	float elevation;;
	String state;
	String name;
}

class WeatherData {
	String id;
	int year;
	int month;
	int day;
	String element;
	int value;
	String qflag;

	public String toString() {
		return id + " " + year + " " + month + " " + day + " " + element + " " + value + " " + qflag;

	}
}

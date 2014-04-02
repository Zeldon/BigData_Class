package sg.edu.nus.cs5344.spring14.twitter.datastructure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import sg.edu.nus.cs5344.spring14.twitter.datastructure.collections.HashTagList;

/**
 * A single tweet.
 * This class contains all the information we need about a single tweet.
 * 
 * @author Tobias Bertelsen
 *
 */
public class Tweet implements WritableComparable<Tweet>, Copyable<Tweet> {

	private Time time = new Time();
	private LatLong latLong = new LatLong();
	private HashTagList hashTagList = new HashTagList();

	public Tweet(){
	}

	public Tweet(Time time, LatLong latLong, HashTagList hashTagList){
		time = time.copy();
		latLong = latLong.copy();
		hashTagList = hashTagList.copy();
	}

	/**
	 * Parse a line of the input data.
	 * @param line
	 * @throws IllegalArgumentException if the line cannot be parsed.
	 */
	public Tweet(String line) {
		// TODO: Implement this
		// 1. split line into columns
		// 2. Convert textual time to a Time object
		// 3. Convert Lat and long coordinates to doubles, and create a LatLong object
		// 4. Read all hashtags into a hasTagList

	}



	@Override
	public void write(DataOutput out) throws IOException {
		time.write(out);
		latLong.write(out);
		hashTagList.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		time.readFields(in);
		latLong.readFields(in);
		hashTagList.readFields(in);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Tweet) {
			Tweet other = (Tweet) obj;
			if (time.equals(other.time) &&
					latLong.equals(other.latLong) &&
					hashTagList.equals(other.hashTagList)){
				return true;
			}
		}
		return false;
	}

	@Override
	public int hashCode() {
		int hash = time.hashCode();
		hash += 31*latLong.hashCode();
		hash += 31*31*hashTagList.hashCode();
		return hash;
	}

	@Override
	public int compareTo(Tweet o) {
		int comparison = time.compareTo(o.time);
		if (comparison != 0) {
			return comparison;
		}

		comparison = latLong.compareTo(o.latLong);
		if (comparison != 0) {
			return comparison;
		}

		return hashTagList.compareTo(o.hashTagList);
	}

	@Override
	public Tweet copy() {
		return new Tweet(time, latLong, hashTagList);
	}
}

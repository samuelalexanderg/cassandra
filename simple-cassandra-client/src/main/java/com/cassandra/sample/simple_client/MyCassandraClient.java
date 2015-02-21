package com.cassandra.sample.simple_client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Hello world!
 *
 */
public class MyCassandraClient {
	private Cluster cluster;
	
	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		System.out.println("Cluster Name " + cluster.getClusterName());
		cluster.connect();
	}

	/**
	 * cqlsh:sample> select * from users;

 	user_id | fname   | lname
---------+---------+----------
       1 |     Sam | Gnanaraj
       2 |  Sheeba |      Sam
       4 | Jovelyn |      Sam
       3 |  Joshua |      Sam

	 */
	public void readUsers() {
		Session sampleSession = cluster.connect("sample");
		ResultSet results = sampleSession.execute("select * from users;");
		System.out.println("Total users " + results.getAvailableWithoutFetching());
		for (Row row : results) {
			System.out.println("Fetched Row " + row);
			System.out.println("User Id " + row.getInt("user_id"));
			System.out.println("First Name " + row.getString("fname"));
			System.out.println("Last Name " + row.getString("lname"));
		}
	}
	
	public static void main( String[] args ) {
		MyCassandraClient client = new MyCassandraClient();
		client.connect("127.0.0.1");
		client.readUsers();
    }
}

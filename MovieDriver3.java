package ics_370_iteration4;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;


public class MovieDriver3 {
	public static boolean processMovieSongs() {
		boolean state = false;
		Connection con = null;
		
		try {
			String dbURL = "jdbc:mysql://localhost:3306/omdb2";
			String username = "root";
			String password = "admin";
			con = DriverManager.getConnection(dbURL, username, password);

			String movieStatement = "";
			String movieStatement2 = "";
			String movieStatement3 = "";

			
			//movies**************************************************************
			Statement st = con.createStatement();
			String sql = "select * from movies";
			ResultSet rs = st.executeQuery(sql);
			ResultSetMetaData metaData = rs.getMetaData();
			int rowCount = metaData.getColumnCount();

			boolean exist_Not = false;
			String native_name = "native_name";
			String year_made = "year_made";

			for (int i = 1; i <= rowCount; i++)
				for (int j = 1; j <= rowCount; j++) {
					if (native_name.equals(metaData.getColumnName(i)) && year_made.equals(metaData.getColumnName(j))){
						exist_Not = true;
						movieStatement = "[2] M ignored ";
						System.out.println("M ignored");
						}
					}
			if (!exist_Not) {
				st.executeUpdate("alter table movies " + "add column native_name varchar(20), " + "add column year_made int");					
				movieStatement = "[1] M created ";
				System.out.println("M created");
			}
			
			
			//song**************************************************************
			Statement st1 = con.createStatement();
			String sql1 = "select * from songs";
			ResultSet rs1 = st1.executeQuery(sql1);
			ResultSetMetaData metaData1 = rs1.getMetaData();
			int rowCount1 = metaData1.getColumnCount();

			boolean exist_Not1 = false;
			String title = "title";

			for (int i = 1; i <= rowCount1; i++){
				if (title.equals(metaData1.getColumnName(i))) {
					exist_Not1 = true;
					movieStatement2 = "[4] S ignored ";
					System.out.println("S ignored");
					}
				}
			if (!exist_Not1) {
				st1.executeUpdate("alter table songs" + "add column title varchar(50)");					
				movieStatement2 = "[3] S created ";
				System.out.println("S created");
			}
			
			
			//movie_song**************************************************************
			Statement st2 = con.createStatement();
			String sql2 = "select * from movie_song";
			ResultSet rs2 = st2.executeQuery(sql2);
			ResultSetMetaData metaData2 = rs2.getMetaData();
			int rowCount2 = metaData2.getColumnCount();

			boolean exist_Not2 = false;
			String movie_id = "movie_id";
			String song_id = "song_id";

			for (int i = 1; i <= rowCount2; i++)
				for (int j = 1; j <= rowCount2; j++) {
					if (movie_id.equals(metaData2.getColumnName(i)) && song_id.equals(metaData2.getColumnName(j))){
						exist_Not2 = true;
						movieStatement3 = "[6] MS ignored";
						System.out.println("MS ignored");
						}
					}
			if (!exist_Not2) {
				st2.executeUpdate("alter table movie_song " + "add column movie_id int, " + "add column song_id int");					
				movieStatement3 = "[5] MS created";
				System.out.println("MS created");
			}
			
			
			//ms_test_data**************************************************************
			String sql4 = "UPDATE ms_test_data SET execution_status=?";
			PreparedStatement statement4 = con.prepareStatement(sql4);
			statement4.setString(1, movieStatement + movieStatement2 + movieStatement3);
			con.close();
			state = true;
			return state;
			} catch (Exception ex) {
				ex.printStackTrace();
				return false;
				} finally {
					try {
						if (con != null)
							con.close();
						} catch (Exception ex) {
							ex.printStackTrace();
							return false;
							}
					}
		}
	public static void main(String[] args) {
		processMovieSongs();
	}
}
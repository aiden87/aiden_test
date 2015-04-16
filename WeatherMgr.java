package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class WeatherMgr extends WeatherLogger{

	private String seperator = null;
	private String filename = null;
	
	private static final String SQL = 
		"INSERT INTO CRET040 " +
		" (" +
		"   AREA_CD, 	WT_DT, 	 AREA_KOR_NM, 	AREA_ENG_NM, AVG_TEMP, MIN_TEMP, MAX_TEMP," +
		"   RAIN, 		SNOW,    WT_TEXT, 		ICON, 		 IN_USER, 	 IN_TIME " +
		" ) VALUES (" +
		" 	?, ?, ?, ?, ?, ?, ?," +
		"	?, ?, ?, ?, 'DAEMON', SYSDATE " +
		" )";
	
	public WeatherMgr(String url, String user, String passwd, String weatherDt, String curTime ) {
		
		super(url, user, passwd, weatherDt, curTime);
		
	}
	
	public void run() throws Exception {
		
		BufferedReader in =	null;
		Connection con = null;
		PreparedStatement pstmt = null;
		
		String text = new String();
		String arrVal[] = null;
		int cnt = 0;
		
		try {
			// 1. 파일 오픈
			in = new BufferedReader(new InputStreamReader(new FileInputStream(new File(filename))));
			
			// 2. DB Connection
			con = DriverManager.getConnection( getUrl(), getUser(), getPasswd());
			con.setAutoCommit(false);
			pstmt = con.prepareStatement(SQL);
			
			while ((text = in.readLine()) != null)
			{
				//System.out.println (text);
				
				cnt = cnt + 1;
				arrVal = text.split(seperator);
				//for (int i=0; i<arrVal.length; i++) System.out.println (arrVal[i]);
				
				// 1: AREA_CD, 2: WT_DT, 3:AREA_KOR_NM, 4:AREA_ENG_NM, 5:AVG_TEMP, 6:MIN_TEMP, 7:MAX_TEMP, 8:RAIN, 9:SNOW, 10:WT_TEXT, 11:ICON 
				// 0: 지역코드 2:한글 3:영문 4:년 ,5:월, 6:일, 7:요일, 8:평균온도, 9:최저기온, 10:최고기온, 11:날씨경향, 12.아이콘, 13:강수량, 14:적설 
				pstmt.setString(1, arrVal[0]);							// 지역코드
				pstmt.setString(2, arrVal[4] + arrVal[5] + arrVal[6]);	// 일자
				pstmt.setString(3, arrVal[2]);							// 한글
				pstmt.setString(4, arrVal[3]);							// 영문
				pstmt.setString(5, arrVal[8]);							// 평균온도
				pstmt.setString(6, arrVal[9]);							// 최저기온
				pstmt.setString(7, arrVal[10]);							// 최고기온
				pstmt.setString(8, arrVal[13]);							// 강수량
				pstmt.setString(9, arrVal[14]);							// 적설
				pstmt.setString(10, arrVal[11]);						// 날씨경향
				pstmt.setString(11, arrVal[12]);						// 아이콘
				
				pstmt.executeUpdate();
				pstmt.clearParameters();
			}
			
			//RunWeather.printer.println("INFO: Succ Weather Insert. Total ROW: " + cnt);
			
			// 커밋.
			con.commit();
			
			super.writeLog(OK, "날씨정보 INSERT CRET040 성공. 건수: " + cnt);
			
		} catch (IOException e) {
			super.writeLog(ERROR, "FILE Not Open. fileName: " + filename + "\n" + e.getMessage());
			//RunWeather.printer.println("ERROR: FILE Not Open. fileName: " + filename);
			con.rollback();
			throw e;
		} catch (SQLException e) {
			super.writeLog(ERROR, "INSERT CRET040 ERROR line: " + cnt + "\n" + e.getMessage());
			//RunWeather.printer.println("ERROR: INSERT ERROR line: " + cnt);
			con.rollback();
			throw e;			
		} catch (Exception e) {
			super.writeLog(ERROR, e.getMessage());
			con.rollback();
			throw e;
		} finally {
			if (pstmt != null)	pstmt.close();
			if (con != null)	con.close();
			if (in != null)		in.close();
		}
	}

	public String getSeperator() {
		return seperator;
	}

	public void setSeperator(String seperator) {
		this.seperator = seperator;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}
}


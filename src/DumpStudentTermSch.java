import java.io.*;
import java.sql.*;
import java.util.*;
import org.apache.log4j.*;
import beans.*;

/*
 * Dumps every student with active term record for the term passed in as 
 * first arg, or else the last term to begin, whichever is later term.
 * This version includes primary ac prog (school) in the feed.
 */
public class DumpStudentTermSch {


    private Properties config;
    private Connection ODSConn;
    private Connection localConn;

    static final Logger logger  = LogManager.getLogger(DumpStudentTermSch.class.getName());

    public DumpStudentTermSch() {
	initConfiguration("config/patron-db.conf");
	ODSConn = initDbConnection("ods");
	
    }


    private void loadSISData(PrintWriter studentTermWriter, String selectTerm) {
        String[] psParts = {
        "SELECT ",
		"students.university_id, ",
        	"students.university_computing_id, ",
		"students.primary_last_name, ", 
		"students.primary_first_name, ",
		"students.primary_middle_name, ",
		"students.mailing_address_1 as ma1, ",
		"students.mailing_address_2 as ma2, ",
		"students.mailing_address_3 as ma3, ",
		"students.mailing_address_city as macity, ",
		"students.mailing_address_state as mast, ",
		"students.mailing_address_zipcode as mazip, ",
		"students.phone_preferred as phpref, ",
		"students.home_address_1 as ha1, ",
		"students.home_address_2 as ha2, ",
		"students.home_address_3 as ha3, ",
		"students.home_address_city as hacity, ",
		"students.home_address_state as hast, ",
		"students.home_address_zipcode as hazip, ",
		"students.home_address_country_desc as hacntry, ",
		"students.email_address_uva as email, ",
		"students.active_student_flag, ",
        	"students.student_system_id,  ",
		"max_career, ",
        	"term_description, ",
		"school, ", 
		"students.email_address_preferred ",
        "FROM ",
        	"SYSADM.UV_SR_STUDENTS students, ",


		"(SELECT  ",
			"MIN(academic_career) as max_career , ",
			"student_system_id as term_stu_sys_id, " ,
			"term_desc as term_description, ",
			"primary_academic_program as school, ",
			"term ",
			"FROM ",
			"SYSADM.UV_SR_STUDENT_TERM_NON_ACAD ",
			"WHERE ",
        			"term = ?  ",
			"GROUP BY student_system_id, term, term_desc, primary_academic_program )  ",


        "WHERE ",
		"students.student_system_id = term_stu_sys_id  AND ",
        	"students.Active_Student_Flag = 'Y' AND  ",
		"students.university_id IS NOT NULL AND ",
		"students.university_computing_id IS NOT NULL ",
	" ORDER BY students.university_id ASC" };
    
	String psWhole = "";

	java.sql.Timestamp updateTS = new java.sql.Timestamp(System.currentTimeMillis());
	PreparedStatement ps = null;
	ResultSet rs = null;
	for (int i = 0; i < psParts.length; i++) {
		psWhole = psWhole.concat(psParts[i]);
	}
	logger.debug(psWhole);

	int termStudents = 0;

	try {
	    ps = ODSConn.prepareStatement(psWhole);
	    ps.setString(1,selectTerm);
	    rs = ps.executeQuery();
	    String fileLine;
	    String holder = "";
	    while (rs.next()) {
		fileLine = 
		  rs.getString("university_id") + "^" +
		  rs.getString("university_computing_id") + "^" +
		  rs.getString("primary_last_name") + "^" +
		  rs.getString("primary_first_name") + "^" +
		  rs.getString("primary_middle_name") + "^" +
		  (((holder = rs.getString("ma1")) == null)? "":holder)  + "^" +
		  (((holder = rs.getString("ma2")) == null)? "":holder)  + "^" +
		  (((holder = rs.getString("ma3")) == null)? "":holder)  + "^" +
		  (((holder = rs.getString("macity")) == null)? "":holder)  + "^" +
		  (((holder = rs.getString("mast")) == null)? "":holder)  + "^" +
		  (((holder = rs.getString("mazip")) == null)? "":holder)  + "^" +
		  (((holder = rs.getString("phpref")) == null)? "":holder)  + "^" +
		  (((holder = rs.getString("ha1")) == null)? "":holder)  + "^" +
		  (((holder = rs.getString("ha2")) == null)? "":holder)  + "^" +
		  (((holder = rs.getString("ha3")) == null)? "":holder)  + "^" +
		  (((holder = rs.getString("hacity")) == null)? "":holder)  + "^" +
		  (((holder = rs.getString("hast")) == null)? "":holder)  + "^" +
		  (((holder = rs.getString("hazip")) == null)? "":holder)  + "^" +
		  (((holder = rs.getString("hacntry")) == null)? "":holder)  + "^" +
		  (((holder = rs.getString("email")) == null)? "":holder)  + "^" +
		  rs.getString("active_student_flag") + "^" +
		  rs.getString("max_career") + "^" +
		  rs.getString("term_description") + "^" +
		  (((holder = rs.getString("email_address_preferred")) == null)? "":holder)  + "^" +
		  rs.getString("school") ; 
/*
		  (((holder = rs.getString("email_address_preferred")) == null)? "":holder) ;
*/
		studentTermWriter.println(fileLine);
			
	        termStudents++;
	    }
	    logger.debug("wrote out " + termStudents + " students");
	    ps.close();
	    rs.close();
	} catch (Exception e) {
	    errorExit("Error reading SIS input", e);
	} finally {
	    if (ps != null) {
		try {
		    ps.close();
		    rs.close();
		}
		catch(Exception e) {}
	    }
	}
    }

    private String getCurrentTerm() {
	java.sql.Date now_dt = new java.sql.Date(System.currentTimeMillis());
	String thisTerm = "3001";
	String[] psParts = {
		"SELECT ",
		"MAX(strm) as this_term ",
		"FROM PS_TERM_TBL ",
		"WHERE ",
		"acad_career = 'UGRD' AND ",
		"term_begin_dt < ? " };
	String psWhole ="";
	PreparedStatement ps = null;
	ResultSet rs = null;
	for (int i = 0; i < psParts.length; i++ ) {
		psWhole = psWhole.concat(psParts[i]);
	}
	logger.debug("query: " + psWhole);
	try {
		ps = ODSConn.prepareStatement(psWhole);
		ps.setDate(1,now_dt);
		rs = ps.executeQuery();
		while (rs.next()) {	
			thisTerm = rs.getString("this_term");
		}
		ps.close();
		rs.close();
	} catch (Exception e) {
            errorExit("Error reading SIS term ", e);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                    rs.close();
                }
                catch(Exception e) {}
            }
	    return(thisTerm);
        }
    }

			 
	


    private void initConfiguration( String fileName ) {
	this.config = new Properties();
	try {
	    InputStream is = this.getClass().getResourceAsStream( fileName );
	    this.config.load( is );
	    is.close();
	}
	catch( Exception e ){
	    errorExit( "Unable to initialize the configuration."  , e );
	}
    }

    private Connection initDbConnection( String prefix ) {
	Connection ret = null;
	try {
		Class.forName( config.getProperty( prefix + ".driver" ) );
		logger.debug("prepping connection for prefix " + prefix + " user " +
			config.getProperty( prefix + ".user") + " password " +
			config.getProperty( prefix + ".password" )  + " url " +
			config.getProperty( prefix + ".url" )  );
		ret = DriverManager.getConnection(
						  config.getProperty( prefix + ".url" ) ,
						  config.getProperty( prefix +  ".user" ) ,
						  config.getProperty( prefix + ".password" ) );
		logger.debug("got connection" + ret.toString());
	} catch( Exception e ) {
	    errorExit( "Unable to initialize a database connection."  , e );
	}
	return ret;
    }

    private void errorExit( String message , Exception e ) {
	logger.error( message );
	logger.error( e.toString() );
	e.printStackTrace();
	System.exit(1);
    }

    public static void main(String[] args) throws IOException {
	String term = "0991";
	String studentFileName = "~/sirsi-feed-student";
	PrintWriter studentTermWriter = null;
	PropertyConfigurator.configure("config/patronload-log4j.properties");
	if (args.length < 1 ) {
		logger.info("No term specified on command line.");
	} else {
		term = args[0].trim();
		logger.info("Loading SIS data for term " + term);
	}
	if (args.length > 1 ) {
		studentFileName = args[1].trim();
		logger.info("output file specified: " + studentFileName);
	}
	try {
		studentTermWriter = new PrintWriter(studentFileName);
	} catch ( Exception fileException ) {
		logger.error(" Unable to open output file " + studentFileName, fileException);
		System.exit(3);
	}
	DumpStudentTermSch myLoad = new DumpStudentTermSch();
/*      
 *	Check to see if we've passed the start of a new term 
 *      w/o remembering to bump the arg in the script.
 */
	String SISTerm = myLoad.getCurrentTerm();
	if (term.compareTo(SISTerm) < 0 ) {
		logger.info("Loading SIS data for newer computed term " + SISTerm);
		term = SISTerm;
	}

	logger.info("ods load start");
	myLoad.loadSISData(studentTermWriter, term);
	studentTermWriter.close();
	logger.info( "ods load end");
    }
}

import java.io.*;
import java.sql.*;
import java.util.*;
import org.apache.log4j.*;
import beans.*;

/*
 * Pull students in all courses for given term (cmd line arg)
 * and store them in a patron data base table
 *
 */
public class LoadCourseStudents {


    private Properties config;
    private Connection ODSConn;
    private Connection localConn;

    static final Logger logger  = LogManager.getLogger(LoadCourseStudents.class.getName());

    public LoadCourseStudents() {
	initConfiguration("config/patron-db.conf");
	ODSConn = initDbConnection("ods");
	localConn = initDbConnection("patron");
	
    }


    private void loadSISData(String selectTerm) {
        String[] psParts = {
        "SELECT ",
        	"students.university_computing_id as computing_id, ",
        	"students.student_system_id, ",
        	"enrollment.term, ",
    		"schedule.subject, ",
        	"schedule.catalog_number, ",
		"schedule.class_section as section ",
        "from ",
        	"SYSADM.UV_SR_STUDENTS students ",
    	  "JOIN ",
    	"SYSADM.UV_SR_STUDENT_ENROLLMENT enrollment ",
        		"ON students.student_system_id = enrollment.student_system_id ",
        	  "JOIN ",
    	"SYSADM.UV_SR_Schedule_Of_Classes schedule ",
        		"ON enrollment.term = schedule.term AND ",
    			"enrollment.class_number = schedule.class_number ",
        "where ",
        	"students.Active_Student_Flag = 'Y' AND  ",
        	"enrollment.term = ? AND ",
        	"enrollment.student_enrollment_status = 'Enrolled' AND ",
        	"enrollment.OFFICIAL_GRADE NOT LIKE 'W%' " };
    
	String psWhole = "";

	java.sql.Timestamp updateTS = new java.sql.Timestamp(System.currentTimeMillis());
	PreparedStatement ps = null;
	ResultSet rs = null;
	for (int i = 0; i < psParts.length; i++) {
		psWhole = psWhole.concat(psParts[i]);
	}
	logger.debug(psWhole);

	int courseStudents = 0;
	try {
	    ps = ODSConn.prepareStatement(psWhole);
	    ps.setString(1,selectTerm);
	    rs = ps.executeQuery();
	    while (rs.next()) {
		CourseMember nextCourseMember = new CourseMember();
		nextCourseMember.setComputingID(rs.getString("computing_id"));
		nextCourseMember.setTerm(rs.getString("term"));
		nextCourseMember.setSubject(rs.getString("subject"));
		nextCourseMember.setCatalogNbr(rs.getString("catalog_number"));
		nextCourseMember.setSection(rs.getString("section"));
		nextCourseMember.setRole("S");
		insertCourseStudent(nextCourseMember);
	        courseStudents++;
	    }
	    ps.close();
	    rs.close();
	    logger.debug("inserted " + courseStudents + " into patronDB");
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


    private void insertCourseStudent(CourseMember nextCourseMember) throws SQLException {
        PreparedStatement ps = null;
	try {
            ps = localConn.prepareStatement("INSERT INTO course_member VALUES (?, ?, ?, ?, ?, ?) ");
	    ps.clearParameters();
	    ps.setString(1,	nextCourseMember.getComputingID());
	    ps.setString(2,	nextCourseMember.getRole());
	    ps.setString(3,	nextCourseMember.getTerm());
	    ps.setString(4,	nextCourseMember.getSubject());
	    ps.setString(5,	nextCourseMember.getCatalogNbr());
	    ps.setString(6,	nextCourseMember.getSection());
	    ps.executeUpdate();
        } catch(Exception e) {
	    logger.debug("Insert caught exception " + e.getMessage());
            throw new SQLException();
	} finally {
	    if (ps != null) {
		try {
		    ps.close();
		}
		catch(Exception e) {}
	    }
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
	String term = null;
	PropertyConfigurator.configure("config/patronload-log4j.properties");
	if (args.length < 1 ) {
		logger.error("No term specified on command line.");
		System.exit(2);
	} else {
		term = args[0].trim();
		logger.info("Loading SIS data for term " + term);
	}
	logger.info("ods load start");
	LoadCourseStudents myLoad = new LoadCourseStudents();

	myLoad.loadSISData(term);

	logger.info( "ods load end");
    }
}

package beans;

import org.apache.log4j.*;
import java.util.*;


public class CourseMember {

static final Logger logger = LogManager.getLogger(CourseMember.class.getName());

private String computingID = "";
private String role = "";
private String term = "";
private String classNbr = "";
private String subject = "";
private String catalogNbr = "";
private String section = "";


public String getComputingID() {
	return this.computingID;
}

public String getRole() {
	return this.role;
}

public String getTerm() {
	return this.term;
}

public String getClassNbr() {
	return this.classNbr;
}

public String getSubject() {
	return this.subject;
}

public String getCatalogNbr() {
	return this.catalogNbr;
}

public String getSection() {
	return this.section;
}


public void setComputingID(String computingID) {
	if (computingID != null) {
		this.computingID = computingID;
	}
}

public void setRole(String role) {
	if (role != null) {
		this.role = role;
	}
}

public void setTerm(String term) {
	if (term != null) {
		this.term = term;
	}
}

public void setClassNbr(String classNbr) {
	if (classNbr != null) {
		this.classNbr = classNbr;
	}
}

public void setSubject(String subject) {
	if (subject != null) {
		this.subject = subject;
	}
}

public void setCatalogNbr(String catalogNbr) {
	if (catalogNbr!= null) {
		this.catalogNbr = catalogNbr;
	}
}

public void setSection(String section) {
	if (section != null) {
		this.section = section;
	}
}


	
}

package beans;

import org.apache.log4j.*;
import java.util.*;


public class Patron {

static final Logger logger = LogManager.getLogger(Patron.class.getName());
static final String PATRONFILEDELIM = "\\^";

private String universityID = "";
private String computingID = "";
private String lastName = "";
private String firstName = "";
private String middleName = "";
private String primaryAffiliation = "";
private String department = "";
private String school = "";
private String agencyCD = "";
private String emailAddress = "";
private ArrayList<MailAddress>  mailAddresses;
private ArrayList<PhoneNumber>  phoneNumbers;
private java.util.Date createTS;
private java.util.Date updateTS;


public String getUniversityID() {
	return this.universityID;
}

public String getComputingID() {
	return this.computingID;
}

public String getLastName() {
	return this.lastName;
}

public String getFirstName() {
	return this.firstName;
}

public String getMiddleName() {
	return this.middleName;
}

public String getPrimaryAffiliation() {
	return this.primaryAffiliation;
}

public String getSchool() {
	return this.school;
}

public String getDepartment() {
	return this.department;
}

public String getAgencyCD() {
	return this.agencyCD;
}

public String getEmailAddress() {
	return this.emailAddress;
}

public ArrayList<MailAddress> getMailAddresses() {
	return this.mailAddresses;
}

public ArrayList<PhoneNumber> getPhoneNumbers() {
	return this.phoneNumbers;
}

public void setUniversityID(String universityID) {
	if (universityID != null) {
		this.universityID = universityID;
	}
}

public void setComputingID(String computingID) {
	if (computingID != null) {
		this.computingID = computingID;
	}
}

public void setLastName(String lastName) {
	if (lastName != null) {
		this.lastName = lastName;
	}
}

public void setFirstName(String firstName) {
	if (firstName != null) {
		this.firstName = firstName;
	}
}

public void setMiddleName(String middleName) {
	if (middleName != null) {
		this.middleName = middleName;
	}
}

public void setPrimaryAffiliation(String primaryAffiliation) {
	if (primaryAffiliation != null) {
		this.primaryAffiliation = primaryAffiliation;
	}
}

public void setSchool(String school) {
	if (school != null) {
		this.school = school;
	}
}

public void setDepartment(String department) {
	if (department != null) {
		this.department = department;
	}
}

public void setAgencyCD(String agencyCD) {
	if (agencyCD != null) {
		this.agencyCD = agencyCD;
	}
}

public void setEmailAddress(String emailAddress) {
	if (emailAddress != null) {
		this.emailAddress = emailAddress;
	}
}

public void setMailAddresses(ArrayList<MailAddress> mailAddresses) {
	if (mailAddresses != null) {
		this.mailAddresses = mailAddresses;
	}
}

public void setPhoneNumbers(ArrayList<PhoneNumber> phoneNumbers) {
	if (phoneNumbers != null) {
		this.phoneNumbers = phoneNumbers;
	}
}

private void userDBPatron(String patronFileLine) { 
	java.util.Date never = new java.util.Date((long)0);
	this.createTS = this.updateTS = never;
	ArrayList<MailAddress>  addresses = new ArrayList<MailAddress>();
	ArrayList<PhoneNumber>  numbers = new ArrayList<PhoneNumber>();
	this.setMailAddresses(addresses);
	this.setPhoneNumbers(numbers);
	if (patronFileLine == null) {
		logger.debug("got empty file line");
		return;
	}
	String[] patronFields = patronFileLine.split(PATRONFILEDELIM);
	if (patronFields.length < 7) {
		logger.debug("file line too short: "  + patronFields.length);
		return;
	}
	this.setUniversityID(patronFields[0].trim());
	this.setComputingID(patronFields[1].trim());
	this.setEmailAddress(patronFields[2].trim());
	this.setLastName(patronFields[3].trim());
	this.setFirstName(patronFields[4].trim());
	this.setMiddleName(patronFields[5].trim());
	this.setPrimaryAffiliation(patronFields[6]);
	String primaff = this.getPrimaryAffiliation();
	/* pick out students */
	if (patronFields.length > 7) {
	    this.setAgencyCD(patronFields[7]);
	}
	if (patronFields.length > 8) {
		if (primaff.endsWith("Student") || primaff.equals("Continuing Education")) {
			this.setSchool(patronFields[8]);
		} else {
			this.setDepartment(patronFields[8]);
		}
	}
	if (patronFields.length == 11) {
		MailAddress mailAddress = new MailAddress();
		mailAddress.setAddressTypeCode("D");
		mailAddress.setAddressLine1(patronFields[9].trim());
		String addressLine2 = patronFields[10];
		/* is line 2 a well formed city, state adress line? */
		int comma = addressLine2.indexOf(',');
		int lastSpace = addressLine2.lastIndexOf(" ");
		if ((comma > 0) && (lastSpace > 0) && 
		    (lastSpace < (addressLine2.length() - 1)) && (comma < lastSpace) ) {
			mailAddress.setCity(addressLine2.substring(0,comma).trim());
			mailAddress.setState(addressLine2.substring(comma+1,lastSpace).trim());
			mailAddress.setPostalCode(addressLine2.substring(lastSpace++).trim());
		}
/* 		no use adding an empty address */
		if (!mailAddress.getAddressLine1().equals("")) {
			addresses.add(mailAddress);
		}
	} else if (patronFields.length == 10) {
		MailAddress mailAddress = new MailAddress();
		mailAddress.setAddressTypeCode("M");
		mailAddress.setAddressLine1(patronFields[9].trim());
/* 		no use adding an empty address */
		if (!mailAddress.getAddressLine1().equals("")) {
			addresses.add(mailAddress);
		}
	}
	this.setMailAddresses(addresses);
}

private void SISPatron(String patronFileLine) {
	java.util.Date never = new java.util.Date((long)0);
	this.createTS = this.updateTS = never;
	ArrayList<MailAddress>  addresses = new ArrayList<MailAddress>();
	ArrayList<PhoneNumber>  numbers = new ArrayList<PhoneNumber>();
	this.setMailAddresses(addresses);
	this.setPhoneNumbers(numbers);
	if (patronFileLine == null) {
		logger.debug("got empty file line");
		return;
	}
	String[] patronFields = patronFileLine.split(PATRONFILEDELIM);
	if (!(patronFields.length == 23)) {
		logger.debug("bad file line " + patronFileLine);
		return;
	}
	this.setUniversityID(patronFields[0].trim());
	this.setComputingID(patronFields[1].trim());
	this.setLastName(patronFields[2].trim());
	this.setFirstName(patronFields[3].trim());
	this.setMiddleName(patronFields[4].trim());
	MailAddress mailAddress = new MailAddress();
	mailAddress.setAddressTypeCode("L");
	mailAddress.setAddressLine1(patronFields[5].trim());
	mailAddress.setAddressLine2(patronFields[6].trim());
	mailAddress.setCity(patronFields[8].trim());
	mailAddress.setState(patronFields[9].trim());
	mailAddress.setPostalCode(patronFields[10].trim());
	addresses.add(mailAddress);
	PhoneNumber phoneNumber = new PhoneNumber();
	phoneNumber.setPhoneTypeCode("P");
	phoneNumber.setPhoneNumber(patronFields[11].trim());
	numbers.add(phoneNumber);
	this.setPhoneNumbers(numbers);
	MailAddress homeAddress = new MailAddress();
	homeAddress.setAddressTypeCode("H");
	homeAddress.setAddressLine1(patronFields[12].trim());
	homeAddress.setAddressLine2(patronFields[13].trim());
	homeAddress.setCity(patronFields[15].trim());
	homeAddress.setState(patronFields[16].trim());
	homeAddress.setPostalCode(patronFields[17].trim());
	homeAddress.setCountry(patronFields[18].trim());
	addresses.add(homeAddress);
	this.setMailAddresses(addresses);
	this.setEmailAddress(patronFields[19].trim());
	this.setPrimaryAffiliation(patronFields[21].trim());

	

}
	

public Patron(String patronFileLine,String fileSource) {
	if (fileSource == null) {
		logger.error("null fileSource, exiting");
		return;
	}
	if (fileSource.equals("userDB")) {
		userDBPatron(patronFileLine);
	} else if (fileSource.equals("SIS")) {
		SISPatron(patronFileLine);
	} else {
		logger.error("bad file source: " + fileSource + " exiting");
		return;
	}
}

}

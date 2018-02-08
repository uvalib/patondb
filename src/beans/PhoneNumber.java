package beans;

import org.apache.log4j.*;
import java.util.*;


public class PhoneNumber {

static final Logger logger = LogManager.getLogger(PhoneNumber.class.getName());

String phoneTypeCode = "";
String phoneNumber = "";


public String getPhoneTypeCode() {
	return this.phoneTypeCode;
} 

public String getPhoneNumber() {
	return this.phoneNumber;
}

public void setPhoneTypeCode(String phoneTypeCode) {
	if (!(phoneTypeCode == null)) {
		this.phoneTypeCode = phoneTypeCode;
	}
}

public void setPhoneNumber(String phoneNumber) {
	if (!(phoneNumber == null)) {
		this.phoneNumber = phoneNumber;
	}
}

}

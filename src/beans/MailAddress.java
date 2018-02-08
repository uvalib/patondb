package beans;

import org.apache.log4j.*;
import java.util.*;


public class MailAddress {

static final Logger logger = LogManager.getLogger(MailAddress.class.getName());

private String addressTypeCode = "";
private String addressLine1 = "";
private String addressLine2 = "";
private String addressLine3 = "";
private String city = "";
private String state = "";
private String country = "";
private String postalCode = "";


public String getAddressTypeCode() {
	return this.addressTypeCode;
} 

public String getAddressLine1() {
	return this.addressLine1;
}

public String getAddressLine2() {
	return this.addressLine2;
}

public String getAddressLine3() {
	return this.addressLine3;
}

public String getCity() {
	return this.city;
}

public String getState() {
	return this.state;
}

public String getCountry() {
	return this.country;
}

public String getPostalCode() {
	return this.postalCode;
}

public void setAddressTypeCode(String addressTypeCode) {
	if (this.addressTypeCode != null) {
		this.addressTypeCode = addressTypeCode;
	}
}


public void setAddressLine1(String addressLine1) {
	if (addressLine1 != null ) {
		this.addressLine1 = addressLine1;
	}
}

public void setAddressLine2(String addressLine2) {
	if (addressLine2 != null ) {
		this.addressLine2 = addressLine2;
	}
}

public void setAddressLine3(String addressLine3) {
	if (addressLine3 != null ) {
		this.addressLine3 = addressLine3;
	}
}

public void setCity(String city) {
	if (city != null) {
		this.city = city;
	}
}

public void setState(String state) {
	if (state != null) {
		this.state = state;
	}
}

public void setCountry(String country) {
	if (country != null) {
		this.country = country;
	}
}

public void setPostalCode(String postalCode) {
	if (postalCode != null) {
		this.postalCode = postalCode;
	}
}


}

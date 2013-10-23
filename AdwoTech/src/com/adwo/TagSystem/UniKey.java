/**
 * 
 */
package com.adwo.TagSystem;

/**
 * @author dev
 *
 */
public class UniKey {

	private String UniKeyName;
	private Double UniKeyPossibility = 0.0;
	private Tag adtag;
	private Tag prgtag;
	private Udid udid;
	/**
	 * @return the uniKeyName
	 */
	public String getUniKeyName() {
		return UniKeyName;
	}
	/**
	 * @param uniKeyName the uniKeyName to set
	 */
	public void setUniKeyName(String uniKeyName) {
		UniKeyName = uniKeyName;
	}
	/**
	 * @return the uniKeyShow
	 */
	/**
	 * @return the uniKeyPossibility
	 */
	public Double getUniKeyPossibility() {
		return UniKeyPossibility;
	}
	/**
	 * @param uniKeyPossibility the uniKeyPossibility to set
	 */
	public void setUniKeyPossibility(Double uniKeyPossibility) {
		UniKeyPossibility = uniKeyPossibility;
	}
	
	/**
	 * @return the udid
	 */
	public Udid getUdid() {
		return udid;
	}
	/**
	 * @param udid the udid to set
	 */
	public void setUdid(Udid udid) {
		this.udid = udid;
	}
	/**
	 * @return the prgtag
	 */
	public Tag getPrgtag() {
		return prgtag;
	}
	/**
	 * @param prgtag the prgtag to set
	 */
	public void setPrgtag(Tag prgtag) {
		this.prgtag = prgtag;
	}
	/**
	 * @return the adtag
	 */
	public Tag getAdtag() {
		return adtag;
	}
	/**
	 * @param adtag the adtag to set
	 */
	public void setAdtag(Tag adtag) {
		this.adtag = adtag;
	}
}

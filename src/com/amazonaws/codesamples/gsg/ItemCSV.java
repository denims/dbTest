package com.amazonaws.codesamples.gsg;

import com.amazonaws.services.dynamodbv2.document.Item;

public class ItemCSV extends Item{

	/* (non-Javadoc)
	 * @see com.amazonaws.services.dynamodbv2.document.Item#toString()
	 */
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		String name = this.getString("Name");
		String desig = this.getString("Designation");
		String out = name + "," + desig;
		return out;
	}
	
	

}

package org.billcc.lineage.hive.hook.lineage.bean;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Column block
 */
public class ColumnBlock {
	/**
	 * column condition
	 */
	private String condition;
	
	/**
	 * column list
	 */
	private Set<String> columnSet = new LinkedHashSet<String>();
	
	public String getCondition() {
		return condition;
	}
	public void setCondition(String condition) {
		this.condition = condition;
	}
	
	public Set<String> getColumnSet() {
		return columnSet;
	}
	public void setColumnSet(Set<String> columnSet) {
		this.columnSet = columnSet;
	}
}
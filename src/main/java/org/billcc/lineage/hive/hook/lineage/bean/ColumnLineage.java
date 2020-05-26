package org.billcc.lineage.hive.hook.lineage.bean;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * column lineage
 */
public class ColumnLineage {
	/**
	 * column name from sql
	 */
	private String toNameParse;
	/**
	 * Source field with condition list
	 */
	//private String colCondition;
	private List<String> colConditions = new ArrayList<>();
	/**
	 * Source field list
	 */
	private Set<String> fromNameSet = new LinkedHashSet<String>();
	/**
	 * Column condition
	 */
	private Set<String> conditionSet = new LinkedHashSet<String>();

	/**
	 * output table
	 */
	private String toTable;
	/**
	 * Column name from metadata
	 */
	private String toName;

	public ColumnLineage(String toNameParse, List<String> colConditions, Set<String> fromNameSet, Set<String> conditionSet,
			String toTable, String toName) {
		this.toNameParse = toNameParse;
		this.colConditions = colConditions;
		this.fromNameSet = fromNameSet;
		this.conditionSet = conditionSet;
		this.toTable = toTable;
		this.toName = toName;
	}

	public String getToNameParse() {
		return toNameParse;
	}

	public void setToNameParse(String toNameParse) {
		this.toNameParse = toNameParse;
	}

	public List<String> getColConditions() {
		return colConditions;
	}
	public void setColConditions(List<String> colConditions) {
		this.colConditions = colConditions;
	}
	public void addColCondition(String condition) {
		this.colConditions.add(condition);
	}
	public void addColCondition(List<String> conditions) {
		if(conditions != null) {
			this.colConditions.addAll(conditions);
		}
	}

	/**
	public String getColCondition() {
		return colCondition;
	}
	public void setColCondition(String colCondition) {
		this.colCondition = colCondition;
	}
	public List<String> getColConditionArray() {
		if (StringUtils.isNotBlank(colCondition)) {
			String[] ccList = colCondition.split("@&@");
			return Arrays.asList(ccList);
		}
		return Collections.emptyList();
	}
	*/

	public Set<String> getFromNameSet() {
		return fromNameSet;
	}

	public void setFromNameSet(Set<String> fromNameSet) {
		this.fromNameSet = fromNameSet;
	}

	public Set<String> getConditionSet() {
		return conditionSet;
	}

	public void setConditionSet(Set<String> conditionSet) {
		this.conditionSet = conditionSet;
	}

	public String getToTable() {
		return toTable;
	}

	public void setToTable(String toTable) {
		this.toTable = toTable;
	}

	public String getToName() {
		return toName;
	}

	public void setToName(String toName) {
		this.toName = toName;
	}

	public String getToColumnQualifiedName() {
		return getToTable() + "." + getToNameParse();
	}
}
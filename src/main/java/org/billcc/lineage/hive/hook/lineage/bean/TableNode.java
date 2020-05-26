package org.billcc.lineage.hive.hook.lineage.bean;

/**
 * table node
 *
 */
public class TableNode {
	/**
	 * table node id
	 */
	private long id;
	/**
	 * table name
	 */
	private String table;
	/**
	 * database name
	 */
	private String db;
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	
	public String getDb() {
		return db;
	}
	public void setDb(String db) {
		this.db = db;
	}
}

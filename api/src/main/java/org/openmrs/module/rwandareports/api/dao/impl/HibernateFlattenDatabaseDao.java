package org.openmrs.module.rwandareports.api.dao.impl;

import org.openmrs.api.db.hibernate.DbSessionFactory;
import org.openmrs.module.rwandareports.api.dao.FlattenDatabaseDao;

/**
 * @author smallGod
 * @date: 01/03/2023
 */
public class HibernateFlattenDatabaseDao implements FlattenDatabaseDao {
	
	private DbSessionFactory sessionFactory;
	
	@Override
	public void executeFlatteningScript() {
		
		sessionFactory.getCurrentSession().createSQLQuery("CALL sp_mamba_data_processing_etl()").executeUpdate();
	}
	
	public DbSessionFactory getSessionFactory() {
		return sessionFactory;
	}
	
	public void setSessionFactory(DbSessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}
}

/**
 * The contents of this file are subject to the OpenMRS Public License
 * Version 1.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://license.openmrs.org
 * <p>
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * <p>
 * Copyright (C) OpenMRS, LLC.  All Rights Reserved.
 */
package org.openmrs.module.rwandareports;

import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.api.context.Context;
import org.openmrs.module.BaseModuleActivator;
import org.openmrs.scheduler.SchedulerException;
import org.openmrs.scheduler.Task;
import org.openmrs.module.rwandareports.task.FlattenTableTask;
import org.openmrs.scheduler.SchedulerService;
import org.openmrs.scheduler.TaskDefinition;

/**
 * This class contains the logic that is run every time this module is either started or shutdown
 */
public class RwandaReportsModuleActivator extends BaseModuleActivator {
	
	private static Log log = LogFactory.getLog(RwandaReportsModuleActivator.class);
	
	@Override
	public void started() {
		log.info("Started Rwanda Report Module Config");
		registerTask("Register Reports", "Registers report definitions", RegisterReportsTask.class, 60 * 60 * 24l);
		
		log.info("Started OHRI-MambaETL");
		System.out.println("Adding mamba flattening Task...");
		registerTask("MambaETL Reporting Task", "MambaETL - flatten data models & Prepare Reporting data",
		    FlattenTableTask.class, 60 * 60 * 24l);
	}
	
	@Override
	public void stopped() {
		log.info("Stopped Rwanda Report Module");
	}
	
	void addTask(String name, String className, Long repeatInterval, String description) {
		
		SchedulerService scheduler = Context.getSchedulerService();
		TaskDefinition taskDefinition = scheduler.getTaskByName(name);
		if (taskDefinition == null) {
			
			taskDefinition = new TaskDefinition(null, name, description, className);
			taskDefinition.setStartOnStartup(Boolean.TRUE);
			taskDefinition.setRepeatInterval(repeatInterval);
			scheduler.saveTaskDefinition(taskDefinition);
		}
	}
	
	/**
	 * Register a new OpenMRS task
	 * 
	 * @param name the name
	 * @param description the description
	 * @param clazz the task class
	 * @param interval the interval in seconds
	 * @return boolean true if successful, else false
	 * @throws SchedulerException if task could not be scheduled
	 */
	private static boolean registerTask(String name, String description, Class<? extends Task> clazz, long interval) {
		try {
			Context.addProxyPrivilege("Manage Scheduler");
			
			TaskDefinition taskDef = Context.getSchedulerService().getTaskByName(name);
			if (taskDef == null) {
				Calendar cal = Calendar.getInstance();
				cal.add(Calendar.MINUTE, 20);
				taskDef = new TaskDefinition();
				taskDef.setTaskClass(clazz.getCanonicalName());
				taskDef.setStartOnStartup(true);
				taskDef.setRepeatInterval(interval);
				taskDef.setStarted(true);
				taskDef.setStartTime(cal.getTime());
				taskDef.setName(name);
				taskDef.setUuid(UUID.randomUUID().toString());
				taskDef.setDescription(description);
				Context.getSchedulerService().scheduleTask(taskDef);
			}
			
		}
		catch (SchedulerException ex) {
			log.warn("Unable to register task '" + name + "' with scheduler", ex);
			return false;
		}
		finally {
			Context.removeProxyPrivilege("Manage Scheduler");
		}
		return true;
	}
}

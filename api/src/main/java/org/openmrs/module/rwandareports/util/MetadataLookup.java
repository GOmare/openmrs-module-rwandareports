package org.openmrs.module.rwandareports.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.Concept;
import org.openmrs.ConceptName;
import org.openmrs.EncounterType;
import org.openmrs.Form;
import org.openmrs.Location;
import org.openmrs.OrderType;
import org.openmrs.PatientIdentifierType;
import org.openmrs.PersonAttributeType;
import org.openmrs.Program;
import org.openmrs.ProgramWorkflow;
import org.openmrs.ProgramWorkflowState;
import org.openmrs.RelationshipType;
import org.openmrs.api.context.Context;
import org.openmrs.module.reporting.common.ObjectUtil;

public class MetadataLookup {
	
	protected final static Log log = LogFactory.getLog(MetadataLookup.class);
	
	/**
	 * @return the Program that matches the passed uuid, concept name, name, or primary key id
	 */
	public static Program getProgram(String lookup) {
		Program program = Context.getProgramWorkflowService().getProgramByUuid(lookup);
		if (program == null) {
			program = Context.getProgramWorkflowService().getProgramByName(lookup);
		}
		if (program == null) {
			for (Program p : Context.getProgramWorkflowService().getAllPrograms()) {
				if (p.getName().equalsIgnoreCase(lookup)) {
					program = p;
				}
			}
		}
		if (program == null) {
			try {
				program = Context.getProgramWorkflowService().getProgram(Integer.parseInt(lookup));
			}
			catch (Exception e) {}
		}
		if (program == null) {
			throw new IllegalArgumentException("Unable to find program using key: " + lookup);
		}
		
		return program;
	}
	
	/**
	 * @return the ProgramWorkflow matching the given programLookup and workflowLookup
	 */
	public static ProgramWorkflow getProgramWorkflow(String programLookup, String workflowLookup) {
		Program p = getProgram(programLookup);
		ProgramWorkflow wf = p.getWorkflowByName(workflowLookup);
		
		if (wf == null) {
			for (ProgramWorkflow programWorkflow : p.getAllWorkflows()) {
				if (workflowLookup.equalsIgnoreCase(programWorkflow.getConcept().getName().toString())) {
					wf = programWorkflow;
				} else if (workflowLookup.equalsIgnoreCase(programWorkflow.getUuid())) {
					wf = programWorkflow;
				} else if (workflowLookup.equalsIgnoreCase(programWorkflow.getId().toString())) {
					wf = programWorkflow;
				}
			}
		}
		if (wf == null) {
			throw new IllegalArgumentException("Unable to find workflow using " + programLookup + " - " + workflowLookup);
		}
		return wf;
	}
	
	/**
	 * @return the ProgramWorkflowState matching the given programLookup and workflowLookup and
	 *         stateLookup
	 */
	public static ProgramWorkflowState getProgramWorkflowState(String programLookup, String workflowLookup,
	        String stateLookup) {
		ProgramWorkflow wf = getProgramWorkflow(programLookup, workflowLookup);
		ProgramWorkflowState s = wf.getStateByName(stateLookup);
		if (s == null) {
			Concept stateConcept = null;
			try {
				stateConcept = getConcept(stateLookup);
			}
			catch (Exception e) {}
			for (ProgramWorkflowState state : wf.getStates()) {
				if (stateConcept != null && state.getConcept().equals(stateConcept)) {
					s = state;
				} else if (stateLookup.equalsIgnoreCase(state.getUuid())) {
					s = state;
				} else if (stateLookup.equalsIgnoreCase(state.getId().toString())) {
					s = state;
				} else if (stateLookup.equalsIgnoreCase(state.getConcept().getId().toString())) {
					s = state;
				}
			}
		}
		if (s == null) {
			throw new IllegalArgumentException("Unable to find state using " + programLookup + " - " + workflowLookup
			        + " - " + stateLookup);
		}
		return s;
	}
	
	public static List<ProgramWorkflowState> getProgramWorkflowstateList(String lookup) {
		List<ProgramWorkflowState> l = new ArrayList<ProgramWorkflowState>();
		if (ObjectUtil.notNull(lookup)) {
			String[] split = lookup.split(",");
			for (String s : split) {
				int state = Integer.parseInt(s);
				l.add(Context.getProgramWorkflowService().getState(state));
			}
		}
		return l;
	}
	
	/**
	 * @return the PatientIdentifier that matches the passed uuid, name, or primary key id
	 */
	public static PatientIdentifierType getPatientIdentifierType(String lookup) {
		PatientIdentifierType pit = Context.getPatientService().getPatientIdentifierTypeByUuid(lookup);
		if (pit == null) {
			pit = Context.getPatientService().getPatientIdentifierTypeByName(lookup);
		}
		if (pit == null) {
			try {
				pit = Context.getPatientService().getPatientIdentifierType(Integer.parseInt(lookup));
			}
			catch (Exception e) {}
		}
		if (pit == null) {
			throw new RuntimeException("Unable to find Patient Identifier using key: " + lookup);
		}
		return pit;
	}
	
	/**
	 * @return the Concept that matches the passed uuid, name, source:code mapping, or primary key
	 *         id
	 */
	public static Concept getConcept(String lookup) {
		
		// First try to lookup based on UUID
		Concept c = Context.getConceptService().getConceptByUuid(lookup);
		if (c != null) {
			return c;
		}
		
		// Next lookup based on name.  Return any exact match for current locale, otherwise first found concept with matching fully specified name
		List<Concept> possible = Context.getConceptService().getConceptsByName(lookup, Context.getLocale(), false);
		Set<Concept> matches = new HashSet<Concept>();
		for (Concept concept : possible) {
			for (ConceptName conceptName : concept.getNames(false)) {
				if (conceptName.getName().equalsIgnoreCase(lookup)) {
					matches.add(concept);
				}
			}
		}
		if (!matches.isEmpty()) {
			if (matches.size() == 1) {
				return matches.iterator().next();
			} else {
				for (Concept possibleMatch : matches) {
					for (ConceptName conceptName : possibleMatch.getNames(false)) {
						if (conceptName.isFullySpecifiedName()) {
							return possibleMatch;
						}
					}
				}
			}
		}
		
		// Next, try to lookup based on concept mapping, where the lookup is source:term
		try {
			String[] split = lookup.split("\\:");
			if (split.length == 2) {
				c = Context.getConceptService().getConceptByMapping(split[1], split[0]);
				if (c != null) {
					return c;
				}
			}
		}
		catch (Exception e) {}
		
		// Finally try to lookup by id
		try {
			c = Context.getConceptService().getConcept(Integer.parseInt(lookup));
			if (c != null) {
				return c;
			}
		}
		catch (Exception e) {}
		
		throw new IllegalArgumentException("Unable to find Concept using key: " + lookup);
	}
	
	/**
	 * @return the List of Concepts that matches the passed comma-separated list of concept lookups
	 * @see MetadataLookup#getConcept(String)
	 */
	public static List<Concept> getConceptList(String lookup) {
		List<Concept> l = new ArrayList<Concept>();
		if (ObjectUtil.notNull(lookup)) {
			String[] split = lookup.split(",");
			for (String s : split) {
				l.add(MetadataLookup.getConcept(s));
			}
		}
		return l;
	}
	
	/**
	 * @return the List of Concepts that matches the passed any separated list of concept lookups
	 * @see MetadataLookup#getConcept(String)
	 */
	public static List<Concept> getConceptList(String lookup, String separator) {
		List<Concept> l = new ArrayList<Concept>();
		if (ObjectUtil.notNull(lookup)) {
			if (ObjectUtil.notNull(separator)) {
				String[] split = lookup.split(separator);
				for (String s : split) {
					l.add(MetadataLookup.getConcept(s));
				}
			} else {
				l.add(MetadataLookup.getConcept(lookup));
			}
		}
		return l;
	}
	
	/**
	 * @return the Form that matches the passed uuid, name, or primary key id
	 */
	public static Form getForm(String lookup) {
		if (lookup != null) {
			lookup = lookup.trim();
		}
		Form form = Context.getFormService().getFormByUuid(lookup);
		if (form == null) {
			form = Context.getFormService().getForm(lookup);
		}
		if (form == null) {
			try {
				form = Context.getFormService().getForm(Integer.parseInt(lookup));
			}
			catch (Exception e) {}
		}
		if (form == null) {
			throw new IllegalArgumentException("Unable to find Form using key: " + lookup);
		}
		return form;
	}
	
	/**
	 * @return the List of Forms that matches the passed comma-separated list of Form lookups
	 * @see MetadataLookup#getForm(String)
	 */
	public static List<Form> getFormList(String lookup) {
		List<Form> l = new ArrayList<Form>();
		if (ObjectUtil.notNull(lookup)) {
			String[] split = lookup.split(",");
			for (String s : split) {
				l.add(MetadataLookup.getForm(s));
			}
		}
		return l;
	}
	
	/**
	 * @return the List of Forms that matches the passed any separated list of Form lookups
	 * @see MetadataLookup#getForm(String)
	 */
	public static List<Form> getFormList(String lookup, String separator) {
		List<Form> l = new ArrayList<Form>();
		if (ObjectUtil.notNull(lookup)) {
			if (ObjectUtil.notNull(separator)) {
				String[] split = lookup.split(separator);
				for (String s : split) {
					l.add(MetadataLookup.getForm(s));
				}
			} else {
				l.add(MetadataLookup.getForm(lookup));
			}
		}
		return l;
	}
	
	/**
	 * @return the EncounterType that matches the passed uuid, name, or primary key id
	 */
	public static EncounterType getEncounterType(String lookup) {
		EncounterType et = Context.getEncounterService().getEncounterTypeByUuid(lookup);
		if (et == null) {
			et = Context.getEncounterService().getEncounterType(lookup);
		}
		if (et == null) {
			try {
				et = Context.getEncounterService().getEncounterType(Integer.parseInt(lookup));
			}
			catch (Exception e) {}
		}
		if (et == null) {
			throw new IllegalArgumentException("Unable to find EncounterType using key: " + lookup);
		}
		
		return et;
	}
	
	/**
	 * @return the List of EncounterTypes that matches the passed comma-separated list of Encounter
	 *         lookups
	 * @see MetadataLookup#getEncounterType(String)
	 */
	public static List<EncounterType> getEncounterTypeList(String lookup) {
		List<EncounterType> l = new ArrayList<EncounterType>();
		if (ObjectUtil.notNull(lookup)) {
			String[] split = lookup.split(",");
			for (String s : split) {
				l.add(MetadataLookup.getEncounterType(s));
				
			}
		}
		return l;
	}
	
	/**
	 * @return the List of EncounterTypes that matches the passed any separated list of Encounter
	 *         lookups
	 * @see MetadataLookup#getEncounterType(String)
	 */
	public static List<EncounterType> getEncounterTypeList(String lookup, String separator) {
		List<EncounterType> l = new ArrayList<EncounterType>();
		if (ObjectUtil.notNull(lookup)) {
			if (ObjectUtil.notNull(separator)) {
				String[] split = lookup.split(separator);
				for (String s : split) {
					l.add(MetadataLookup.getEncounterType(s));
				}
			} else {
				l.add(MetadataLookup.getEncounterType(lookup));
			}
		}
		return l;
	}
	
	/**
	 * @return the RelationshipType that matches the passed uuid, name, or primary key id
	 */
	public static RelationshipType getRelationshipType(String lookup) {
		RelationshipType rt = Context.getPersonService().getRelationshipTypeByUuid(lookup);
		if (rt == null) {
			rt = Context.getPersonService().getRelationshipTypeByName(lookup);
		}
		if (rt == null) {
			try {
				rt = Context.getPersonService().getRelationshipType(Integer.parseInt(lookup));
			}
			catch (Exception e) {}
		}
		if (rt == null) {
			try {
				rt = Context.getPersonService().getRelationshipTypeByUuid(lookup);
			}
			catch (Exception e) {}
		}
		if (rt == null) {
			throw new IllegalArgumentException("Unable to find RelationshipType using key: " + lookup);
		}
		return rt;
	}
	
	/**
	 * @return the OrderType that matches the passed uuid, name, or primary key id
	 */
	public static OrderType getOrderType(String lookup) {
		OrderType ot = Context.getOrderService().getOrderTypeByUuid(lookup);
		if (ot == null) {
			for (OrderType orderType : Context.getOrderService().getOrderTypes(true)) {
				if (orderType.getName().equalsIgnoreCase(lookup)) {
					ot = orderType;
				}
			}
		}
		if (ot == null) {
			try {
				ot = Context.getOrderService().getOrderType(Integer.parseInt(lookup));
			}
			catch (Exception e) {}
		}
		if (ot == null) {
			throw new IllegalArgumentException("Unable to find OrderType using key: " + lookup);
		}
		return ot;
	}
	
	/**
	 * @return the Location that matches the passed uuid, name, or primary key id
	 */
	public static Location getLocation(String lookup) {
		Location et = Context.getLocationService().getLocationByUuid(lookup);
		if (et == null) {
			et = Context.getLocationService().getLocation(lookup);
		}
		if (et == null) {
			try {
				et = Context.getLocationService().getLocation(Integer.parseInt(lookup));
			}
			catch (Exception e) {}
		}
		if (et == null) {
			throw new IllegalArgumentException("Unable to find Location using key: " + lookup);
		}
		
		return et;
	}
	
	/**
	 * @return the PersonAttributeType that matches the passed uuid, name, or primary key id
	 */
	public static PersonAttributeType getPersonAttributeType(String lookup) {
		PersonAttributeType et = Context.getPersonService().getPersonAttributeTypeByUuid(lookup);
		if (et == null) {
			et = Context.getPersonService().getPersonAttributeTypeByName(lookup);
		}
		if (et == null) {
			try {
				et = Context.getPersonService().getPersonAttributeType(Integer.parseInt(lookup));
			}
			catch (Exception e) {}
		}
		if (et == null) {
			throw new IllegalArgumentException("Unable to find PersonAttributeType using key: " + lookup);
		}
		
		return et;
	}
}

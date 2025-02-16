package org.openmrs.module.rwandareports.reporting;

import org.openmrs.Concept;
import org.openmrs.Program;
import org.openmrs.api.context.Context;
import org.openmrs.module.reporting.cohort.definition.BaseObsCohortDefinition.TimeModifier;
import org.openmrs.module.reporting.common.RangeComparator;
import org.openmrs.module.reporting.common.SortCriteria;
import org.openmrs.module.reporting.common.SortCriteria.SortDirection;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.evaluation.parameter.ParameterizableUtil;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.rowperpatientreports.dataset.definition.RowPerPatientDataSetDefinition;
import org.openmrs.module.rwandareports.renderer.CalendarWebRenderer;
import org.openmrs.module.rwandareports.util.Cohorts;
import org.openmrs.module.rwandareports.util.GlobalPropertiesManagement;
import org.openmrs.module.rwandareports.util.RowPerPatientColumns;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SetupOncologyPediatricAppointmentList extends SingleSetupReport {
	
	//properties retrieved from global variables
	private Program oncologyProgram;
	
	private Concept pediatricChemotherapy;
	
	private Concept pediatricNonChemotherapy;
	
	@Override
	public String getReportName() {
		return "ONC-Oncology Pediatric Appointment List";
	}
	
	public void setup() throws Exception {
		log.info("Setting up report: " + getReportName());
		setupProperties();
		
		ReportDefinition rd = createReportDefinition();
		
		createCustomWebRenderer(rd);
	}
	
	private ReportDefinition createReportDefinition() {
		
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setName(getReportName());
		
		reportDefinition.addParameter(new Parameter("endDate", "Date", Date.class));
		reportDefinition.setBaseCohortDefinition(Cohorts.createInProgramParameterizableByDate("Oncology", oncologyProgram),
		    ParameterizableUtil.createParameterMappings("onDate=${endDate}"));
		createDataSetDefinition(reportDefinition);
		
		Helper.saveReportDefinition(reportDefinition);
		
		return reportDefinition;
	}
	
	private void createDataSetDefinition(ReportDefinition reportDefinition) {
		// Create new dataset definition 
		RowPerPatientDataSetDefinition dataSetDefinition = new RowPerPatientDataSetDefinition();
		dataSetDefinition.setName("Chemotherapy – Pediatric Inpatient Visit Ward");
		
		dataSetDefinition.addParameter(new Parameter("endDate", "endDate", Date.class));
		
		SortCriteria sortCriteria = new SortCriteria();
		sortCriteria.addSortElement("nextRDV", SortDirection.ASC);
		dataSetDefinition.setSortCriteria(sortCriteria);
		
		//Add filters
		dataSetDefinition.addFilter(Cohorts.createDateObsCohortDefinition(pediatricChemotherapy,
		    RangeComparator.GREATER_EQUAL, RangeComparator.LESS_EQUAL, TimeModifier.LAST), ParameterizableUtil
		        .createParameterMappings("value2=${endDate+6M},value1=${endDate-14d}"));
		
		dataSetDefinition.addFilter(Cohorts.createDateObsCohortDefinition(pediatricNonChemotherapy,
		    RangeComparator.GREATER_EQUAL, RangeComparator.LESS_EQUAL, TimeModifier.LAST), ParameterizableUtil
		        .createParameterMappings("value2=${endDate+6M},value1=${endDate-14d}"));
		
		//Add Columns
		dataSetDefinition.addColumn(RowPerPatientColumns.getSystemId("id"), new HashMap<String, Object>());
		
		dataSetDefinition.addColumn(RowPerPatientColumns.getArchivingId("archivingId"), new HashMap<String, Object>());
		
		dataSetDefinition.addColumn(RowPerPatientColumns.getFirstNameColumn("givenName"), new HashMap<String, Object>());
		
		dataSetDefinition.addColumn(RowPerPatientColumns.getMiddleNameColumn("middleName"), new HashMap<String, Object>());
		
		dataSetDefinition.addColumn(RowPerPatientColumns.getFamilyNameColumn("familyName"), new HashMap<String, Object>());
		dataSetDefinition.addColumn(RowPerPatientColumns.getAge("age"), new HashMap<String, Object>());
		
		dataSetDefinition.addColumn(RowPerPatientColumns.getMostRecent("date", pediatricChemotherapy, "yyyy-MM-dd"),
		    new HashMap<String, Object>());
		dataSetDefinition.addColumn(RowPerPatientColumns.getMostRecent("date", pediatricNonChemotherapy, "yyyy-MM-dd"),
		    new HashMap<String, Object>());
		
		dataSetDefinition.addColumn(RowPerPatientColumns.getIMBId("imbId"), new HashMap<String, Object>());
		
		Map<String, Object> mappings = new HashMap<String, Object>();
		mappings.put("endDate", "${endDate}");
		
		reportDefinition.addDataSetDefinition("Chemotherapy – Pediatric Inpatient Visit Ward", dataSetDefinition, mappings);
		
	}
	
	private void setupProperties() {
		
		oncologyProgram = gp.getProgram(GlobalPropertiesManagement.ONCOLOGY_PROGRAM);
		
		pediatricChemotherapy = Context.getConceptService().getConceptByUuid("5efb51db-4e71-497d-822c-91501ac167f6");
		pediatricNonChemotherapy = Context.getConceptService().getConceptByUuid("8c3b045a-aa94-4361-b2e9-8a80c26ccede");
		
	}
	
	private void createCustomWebRenderer(ReportDefinition rd) throws IOException {
		final ReportDesign design = new ReportDesign();
		design.setName("Calendar");
		design.setReportDefinition(rd);
		design.setRendererType(CalendarWebRenderer.class);
		
		Helper.saveReportDesign(design);
		
	}
}

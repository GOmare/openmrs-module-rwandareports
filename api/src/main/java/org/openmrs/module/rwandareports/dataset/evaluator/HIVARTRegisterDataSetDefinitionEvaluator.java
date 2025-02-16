/**
 * The contents of this file are subject to the OpenMRS Public License
 * Version 1.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://license.openmrs.org
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * Copyright (C) OpenMRS, LLC.  All Rights Reserved.
 */
package org.openmrs.module.rwandareports.dataset.evaluator;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.Cohort;
import org.openmrs.DrugOrder;
import org.openmrs.Obs;
import org.openmrs.Patient;
import org.openmrs.annotation.Handler;
import org.openmrs.api.context.Context;
import org.openmrs.module.orderextension.util.OrderEntryUtil;
import org.openmrs.module.reporting.cohort.CohortUtil;
import org.openmrs.module.reporting.cohort.Cohorts;
import org.openmrs.module.reporting.cohort.definition.CohortDefinition;
import org.openmrs.module.reporting.cohort.definition.service.CohortDefinitionService;
import org.openmrs.module.reporting.common.ObjectUtil;
import org.openmrs.module.reporting.data.patient.EvaluatedPatientData;
import org.openmrs.module.reporting.data.patient.definition.PatientObjectDataDefinition;
import org.openmrs.module.reporting.data.patient.service.PatientDataService;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.DataSetColumn;
import org.openmrs.module.reporting.dataset.DataSetRow;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.PatientDataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;
import org.openmrs.module.rowperpatientreports.patientdata.definition.RowPerPatientData;
import org.openmrs.module.rowperpatientreports.patientdata.result.ObservationResult;
import org.openmrs.module.rowperpatientreports.patientdata.result.PatientDataResult;
import org.openmrs.module.rowperpatientreports.patientdata.service.RowPerPatientDataService;
import org.openmrs.module.rwandareports.dataset.HIVARTRegisterDataSetDefinition;
import org.openmrs.module.rwandareports.dataset.HIVRegisterDataSetRowComparator;

/**
 * The logic that evaluates a {@link PatientDataSetDefinition} and produces an {@link DataSet}
 * 
 * @see PatientDataSetDefinition
 */
@Handler(supports = { HIVARTRegisterDataSetDefinition.class })
public class HIVARTRegisterDataSetDefinitionEvaluator implements DataSetEvaluator {
	
	protected Log log = LogFactory.getLog(this.getClass());
	
	private static int START_DATE_WORKFLOW_ART = 0;
	
	private static int START_ART_REGIMEN = 1;
	
	private static int IMB_ID = 2;
	
	private static int TRACNET_ID = 3;
	
	private static int GIVEN_NAME = 4;
	
	private static int FAMILY_NAME = 5;
	
	private static int GENDER = 6;
	
	private static int BIRTHDATE = 7;
	
	private static int AGE_AT_START = 8;
	
	private static int WEIGHT_AT_START = 9;
	
	private static int INITIAL_STAGE = 10;
	
	private static int INITIAL_CD4_COUNT = 11;
	
	private static int INITIAL_CD4_PERCENT = 12;
	
	private static int CTX_TREATMENT = 13;
	
	private static int TB_TREATMENT = 14;
	
	private static int PREGNANCY_DELIVERY_DATES = 15;
	
	private static int INITIAL_REGIMEN = 16;
	
	private static int FIRST_LINE_CHANGES = 17;
	
	private static int SECOND_LINE_CHANGES = 18;
	
	private static int ART_DRUGS = 19;
	
	private static int CD4_OBS = 20;
	
	private static int STAGE_OBS = 21;
	
	private static int TB_OBS = 22;
	
	/**
	 * Public constructor
	 */
	public HIVARTRegisterDataSetDefinitionEvaluator() {
	}
	
	/**
	 * @see DataSetEvaluator#evaluate(DataSetDefinition, EvaluationContext)
	 * @should evaluate a PatientDataSetDefinition
	 */
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext context) throws EvaluationException {
		
		SimpleDataSet dataSet = new SimpleDataSet(dataSetDefinition, context);
		HIVARTRegisterDataSetDefinition definition = (HIVARTRegisterDataSetDefinition) dataSetDefinition;
		
		context = ObjectUtil.nvl(context, new EvaluationContext());
		Cohort cohort = context.getBaseCohort();
		
		// By default, get all patients
		if (cohort == null) {
			cohort = Cohorts.allPatients();
		}
		
		for (CohortDefinition cd : definition.getFilters()) {
			Cohort filter;
			try {
				filter = Context.getService(CohortDefinitionService.class).evaluate(cd, context);
				cohort = CohortUtil.intersect(cohort, filter);
			}
			catch (EvaluationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		// Get a list of patients based on the cohort members
		EvaluationContext allPatientContext = new EvaluationContext();
		allPatientContext.setBaseCohort(cohort);
		EvaluatedPatientData patientData = Context.getService(PatientDataService.class).evaluate(
		    new PatientObjectDataDefinition(), allPatientContext);
		
		for (Object patientObj : patientData.getData().values()) {
			DataSetRow row = new DataSetRow();
			Patient p = (Patient) patientObj;
			
			for (RowPerPatientData pd : definition.getColumns()) {
				pd.setPatient(p);
				pd.setPatientId(p.getPatientId());
				long startTime = System.currentTimeMillis();
				PatientDataResult patientDataResult;
				try {
					patientDataResult = Context.getService(RowPerPatientDataService.class).evaluate(pd, context);
					DataSetColumn c = new DataSetColumn(patientDataResult.getName(), patientDataResult.getDescription(),
					        patientDataResult.getColumnClass());
					row.addColumnValue(c, patientDataResult);
				}
				catch (EvaluationException e) {
					log.debug("Error evaluating dataSet", e);
				}
				long timeTake = System.currentTimeMillis() - startTime;
				log.info(pd.getName() + ": " + timeTake);
			}
			dataSet.addRow(row);
		}
		
		dataSet = transformDataSet(dataSet, dataSetDefinition, context);
		return dataSet;
	}
	
	private SimpleDataSet transformDataSet(DataSet dataset, DataSetDefinition dataSetDefinition, EvaluationContext context) {
		//sort into a list
		List<DataSetRow> rows = new ArrayList<DataSetRow>();
		
		for (DataSetRow row : dataset) {
			rows.add(row);
		}
		
		Collections.sort(rows, new HIVRegisterDataSetRowComparator(dataset));
		
		SimpleDataSet resultSet = new SimpleDataSet(dataSetDefinition, context);
		
		int rowNumber = 0;
		for (DataSetRow row : rows) {
			
			DataSetRow rr = new DataSetRow();
			
			rowNumber++;
			int sheetNumber = 1;
			int startingMonth = 0;
			
			List<DataSetColumn> columnList = dataset.getMetaData().getColumns();
			
			Date startingDate = null;
			DrugOrder startingRegimen = (DrugOrder) ((PatientDataResult) row.getColumnValue(columnList
			        .get(START_ART_REGIMEN))).getValue();
			
			startingDate = (Date) ((PatientDataResult) row.getColumnValue(columnList.get(START_DATE_WORKFLOW_ART)))
			        .getValue();
			
			List<DrugOrder> drugsValue = (List<DrugOrder>) ((PatientDataResult) row
			        .getColumnValue(columnList.get(ART_DRUGS))).getValue();
			List<Obs> cd4Value = (List<Obs>) ((PatientDataResult) row.getColumnValue(columnList.get(CD4_OBS))).getValue();
			List<Obs> stageValue = (List<Obs>) ((PatientDataResult) row.getColumnValue(columnList.get(STAGE_OBS)))
			        .getValue();
			List<Obs> tbValue = (List<Obs>) ((PatientDataResult) row.getColumnValue(columnList.get(TB_OBS))).getValue();
			
			List<DrugOrder> firstLineChange = (List<DrugOrder>) ((PatientDataResult) row.getColumnValue(columnList
			        .get(FIRST_LINE_CHANGES))).getValue();
			List<DrugOrder> secondLineChange = (List<DrugOrder>) ((PatientDataResult) row.getColumnValue(columnList
			        .get(SECOND_LINE_CHANGES))).getValue();
			
			drugsValue = cleanseDrugsList(drugsValue, startingDate);
			cd4Value = cleanseObsList(cd4Value, startingDate);
			stageValue = cleanseObsList(stageValue, startingDate);
			tbValue = cleanseObsList(tbValue, startingDate);
			
			firstLineChange = cleanseDrugsList(firstLineChange, startingDate);
			secondLineChange = cleanseDrugsList(secondLineChange, startingDate);
			
			String firstLineChangeStr = getDiscontinuedReasons(firstLineChange);
			String secondLineChangeStr = getDiscontinuedReasons(secondLineChange);
			
			addPatientRow(rowNumber, startingMonth, sheetNumber, row, rr, dataset, firstLineChangeStr, secondLineChangeStr,
			    drugsValue, cd4Value, stageValue, tbValue);
			resultSet.addRow(rr);
		}
		
		return resultSet;
	}
	
	private void addPatientRow(int rowNumber, int startingMonth, int sheetNumber, DataSetRow row, DataSetRow resultRow,
	        DataSet dataset, String firstLineChange, String secondLineChange, List<DrugOrder> drugsValue,
	        List<Obs> cd4Value, List<Obs> stageValue, List<Obs> tbValue) {
		String colName = "No" + sheetNumber;
		DataSetColumn one = new DataSetColumn(colName, colName, Integer.class);
		resultRow.addColumnValue(one, rowNumber);
		
		Date startingDate = null;
		
		List<DataSetColumn> columnList = dataset.getMetaData().getColumns();
		
		Date workflowChangeDate = startingDate = (Date) ((PatientDataResult) row.getColumnValue(columnList
		        .get(START_DATE_WORKFLOW_ART))).getValue();
		
		DataSetColumn two = new DataSetColumn("Date of Debut of ARV/ART" + sheetNumber, "Date of Debut of ARV/ART",
		        Date.class);
		resultRow.addColumnValue(two, workflowChangeDate);
		
		DrugOrder startingRegimen = (DrugOrder) ((PatientDataResult) row.getColumnValue(columnList.get(START_ART_REGIMEN)))
		        .getValue();
		DataSetColumn three = new DataSetColumn("Date of Starting Regimen" + sheetNumber, "Date of Starting Regimen",
		        Date.class);
		if (startingRegimen == null) {
			resultRow.addColumnValue(three, null);
		} else {
			resultRow.addColumnValue(three, startingRegimen);
		}
		
		startingDate = workflowChangeDate;
		
		for (int j = 2; j < 9; j++) {
			Object cellValue = ((PatientDataResult) row.getColumnValue(columnList.get(j))).getValue();
			
			if (cellValue instanceof ArrayList) {
				cellValue = cellValue.toString();
			}
			if (cellValue instanceof DrugOrder) {
				String drugName = "Drug Missing";
				try {
					drugName = ((DrugOrder) cellValue).getDrug().getName();
				}
				catch (Exception e) {
					System.err.println(e.getMessage());
				}
				cellValue = drugName;
			}
			DataSetColumn col = new DataSetColumn(columnList.get(j).getLabel() + sheetNumber, columnList.get(j).getLabel(),
			        columnList.get(j).getClass());
			resultRow.addColumnValue(col, cellValue);
		}
		
		ObservationResult weightAtStart = (ObservationResult) row.getColumnValue(columnList.get(WEIGHT_AT_START));
		String colLabel = columnList.get(WEIGHT_AT_START).getLabel();
		DataSetColumn weightCol = new DataSetColumn(colLabel + sheetNumber, colLabel, String.class);
		resultRow.addColumnValue(weightCol, weightAtStart.getValue());
		colLabel = colLabel + " date";
		DataSetColumn weightColDate = new DataSetColumn(colLabel + sheetNumber, colLabel, Date.class);
		resultRow.addColumnValue(weightColDate, weightAtStart.getDateOfObservation());
		
		ObservationResult stageAtStart = (ObservationResult) row.getColumnValue(columnList.get(INITIAL_STAGE));
		String stageResult = (String) stageAtStart.getValue();
		//need to remove the "WHO STAGE" from the result
		if (stageResult != null) {
			stageResult = stageResult.replaceFirst("WHO STAGE", "");
		}
		DataSetColumn initStageCol = new DataSetColumn(columnList.get(INITIAL_STAGE).getLabel() + sheetNumber, columnList
		        .get(INITIAL_STAGE).getLabel(), String.class);
		resultRow.addColumnValue(initStageCol, stageResult);
		
		for (int c = 11; c < 13; c++) {
			Object obsResult = row.getColumnValue(columnList.get(c));
			DataSetColumn col = new DataSetColumn(columnList.get(c).getLabel() + sheetNumber, columnList.get(c).getLabel(),
			        String.class);
			
			if (obsResult instanceof ObservationResult) {
				ObservationResult obs = (ObservationResult) row.getColumnValue(columnList.get(c));
				
				String result = obs.getValue();
				if (obs.getDateOfObservation() != null) {
					result = result + " " + new SimpleDateFormat("yyyy-MM-dd").format(obs.getDateOfObservation());
				}
				resultRow.addColumnValue(col, result);
			} else {
				resultRow.addColumnValue(col, null);
			}
		}
		
		for (int k = 13; k < 15; k++) {
			List<DrugOrder> values = (List<DrugOrder>) ((PatientDataResult) row.getColumnValue(columnList.get(k)))
			        .getValue();
			
			String cellValue = "";
			Date startDate = null;
			Date endDate = null;
			for (DrugOrder drO : values) {
				startDate = drO.getEffectiveStartDate();
				endDate = drO.getEffectiveStopDate();
			}
			DataSetColumn startCol = new DataSetColumn("start " + columnList.get(k).getLabel() + sheetNumber, columnList
			        .get(k).getLabel(), Date.class);
			resultRow.addColumnValue(startCol, startDate);
			DataSetColumn endCol = new DataSetColumn("end " + columnList.get(k).getLabel() + sheetNumber, columnList.get(k)
			        .getLabel(), Date.class);
			resultRow.addColumnValue(endCol, endDate);
		}
		
		List<Obs> pregnancy = (List<Obs>) ((PatientDataResult) row.getColumnValue(columnList.get(PREGNANCY_DELIVERY_DATES)))
		        .getValue();
		for (int m = 0; m < 4; m++) {
			String columnName = "Pregnancy " + m + sheetNumber;
			DataSetColumn col = new DataSetColumn(columnName, columnName, String.class);
			
			if (pregnancy != null && pregnancy.size() > m) {
				Obs pregOb = pregnancy.get(m);
				resultRow.addColumnValue(col, pregOb.getValueAsString(Context.getLocale()));
			} else {
				resultRow.addColumnValue(col, null);
			}
		}
		
		DrugOrder initial = (DrugOrder) ((PatientDataResult) row.getColumnValue(columnList.get(INITIAL_REGIMEN))).getValue();
		String drugName = "Drug Missing";
		try {
			drugName = ((DrugOrder) initial).getDrug().getName();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
		}
		DataSetColumn initReg = new DataSetColumn("Initial Regimen" + sheetNumber, "Initial Regimen", String.class);
		resultRow.addColumnValue(initReg, drugName);
		
		DataSetColumn firstLine = new DataSetColumn("First Line Changes" + sheetNumber, "First Line Changes", String.class);
		resultRow.addColumnValue(firstLine, firstLineChange);
		
		DataSetColumn secondLine = new DataSetColumn("Second Line Changes" + sheetNumber, "Second Line Changes",
		        String.class);
		resultRow.addColumnValue(secondLine, secondLineChange);
		
		int month = startingMonth;
		
		String drugCellValue = retrieveCorrectMonthsOb(month, drugsValue, startingDate);
		DataSetColumn monthZero = new DataSetColumn("Month 0" + sheetNumber, "Month 0", String.class);
		resultRow.addColumnValue(monthZero, drugCellValue);
		
		DataSetColumn monthDateZero = new DataSetColumn("Month 0" + sheetNumber + "date", "Month 0 date", String.class);
		if (drugCellValue != null) {
			String cellValues[] = drugCellValue.split(";");
			resultRow.addColumnValue(monthZero, cellValues[0]);
			
			if (cellValues.length > 1) {
				resultRow.addColumnValue(monthDateZero, cellValues[1]);
			}
		} else {
			resultRow.addColumnValue(monthZero, null);
			resultRow.addColumnValue(monthDateZero, null);
		}
		
		for (int f = 0; f < 6; f++) {
			for (int n = 0; n < 5; n++) {
				month++;
				String cellValue = retrieveCorrectMonthsOb(month, drugsValue, startingDate);
				
				String columnName = "Month " + month;
				DataSetColumn monthCol = new DataSetColumn(columnName, columnName, String.class);
				DataSetColumn monthDateCol = new DataSetColumn(columnName + "date", columnName, String.class);
				if (cellValue != null) {
					String cellValues[] = cellValue.split(";");
					resultRow.addColumnValue(monthCol, cellValues[0]);
					
					if (cellValues.length > 1) {
						resultRow.addColumnValue(monthDateCol, cellValues[1]);
					}
				} else {
					resultRow.addColumnValue(monthCol, null);
					resultRow.addColumnValue(monthDateCol, null);
				}
			}
			month++;
			String columnName = "CD4 " + month;
			DataSetColumn monthCol = new DataSetColumn(columnName, columnName, String.class);
			columnName = "CD4 date" + month;
			DataSetColumn monthDateCol = new DataSetColumn(columnName, columnName, String.class);
			
			if (cd4Value != null && cd4Value.size() > 0) {
				List<Obs> valueToBeUsed = retrieveCorrect6MonthsOb(month, cd4Value, startingDate);
				String cellValue = "";
				Date date = null;
				if (valueToBeUsed.size() > 0) {
					for (Obs ob : valueToBeUsed) {
						cellValue = ob.getValueAsString(Context.getLocale());
						date = ob.getObsDatetime();
					}
					
					cd4Value.removeAll(valueToBeUsed);
				}
				resultRow.addColumnValue(monthCol, cellValue);
				resultRow.addColumnValue(monthDateCol, date);
				
			} else {
				resultRow.addColumnValue(monthCol, null);
				resultRow.addColumnValue(monthDateCol, null);
			}
			
			String stageColName = "Stage " + month;
			DataSetColumn stageCol = new DataSetColumn(stageColName, stageColName, String.class);
			if (stageValue != null && stageValue.size() > 0) {
				List<Obs> valueToBeUsed = retrieveCorrect6MonthsOb(month, stageValue, startingDate);
				String cellValue = "";
				if (valueToBeUsed.size() > 0) {
					for (Obs ob : valueToBeUsed) {
						cellValue = ob.getValueAsString(Context.getLocale());
						cellValue = cellValue.replaceFirst("WHO STAGE", "");
						
					}
					
					stageValue.removeAll(valueToBeUsed);
				}
				resultRow.addColumnValue(stageCol, cellValue);
			} else {
				resultRow.addColumnValue(stageCol, null);
			}
			
			String tbColName = "TB " + month;
			DataSetColumn tbCol = new DataSetColumn(tbColName, tbColName, String.class);
			if (tbValue != null && tbValue.size() > 0) {
				List<Obs> valueToBeUsed = retrieveCorrect6MonthsOb(month, tbValue, startingDate);
				String cellValue = "";
				if (valueToBeUsed.size() > 0) {
					for (Obs ob : valueToBeUsed) {
						cellValue = ob.getValueAsString(Context.getLocale());
					}
					
					tbValue.removeAll(valueToBeUsed);
				}
				resultRow.addColumnValue(tbCol, cellValue);
			} else {
				resultRow.addColumnValue(tbCol, null);
			}
		}
		//if we still have cd4, stage, or tb obs left we need to move onto sheet 2
		//or if the drug orders are still current
		String checkForDrugOrders = retrieveCorrectMonthsOb(month + 1, drugsValue, startingDate);
		if ((cd4Value != null && cd4Value.size() > 0) || (stageValue != null && stageValue.size() > 0)
		        || (tbValue != null && tbValue.size() > 0) || checkForDrugOrders.length() > 0) {
			addPatientRow(rowNumber, month, sheetNumber + 1, row, resultRow, dataset, firstLineChange, secondLineChange,
			    drugsValue, cd4Value, stageValue, tbValue);
		}
	}
	
	//to avoid infinite loops we are going to remove all obs that are before the starting date + 3 months
	private List<Obs> cleanseObsList(List<Obs> obs, Date startingDate) {
		List<Obs> obsToReturn = new ArrayList<Obs>();
		//if the starting date is null, we are not going to be able to do any month
		//calculations so we are just going to set the list to null and exit
		if (startingDate != null) {
			
			for (Obs o : obs) {
				int diff = calculateMonthsDifference(o.getObsDatetime(), startingDate);
				
				if (diff > 3) {
					obsToReturn.add(o);
				}
			}
		}
		return obsToReturn;
	}
	
	private List<DrugOrder> cleanseDrugsList(List<DrugOrder> drugOrders, Date startingDate) {
		List<DrugOrder> ordersToReturn = new ArrayList<DrugOrder>();
		//if the starting date is null, we are not going to be able to do any month
		//calculations so we are just going to set the list to null and exit
		if (startingDate != null) {
			Calendar obsResultCal = Calendar.getInstance();
			obsResultCal.setTime(startingDate);
			
			for (DrugOrder o : drugOrders) {
				Calendar oCal = Calendar.getInstance();
				oCal.setTime(o.getEffectiveStartDate());
				
				if ((oCal.get(Calendar.YEAR) == obsResultCal.get(Calendar.YEAR) && oCal.get(Calendar.DAY_OF_YEAR) == obsResultCal
				        .get(Calendar.DAY_OF_YEAR)) || o.getEffectiveStartDate().after(startingDate)) {
					ordersToReturn.add(o);
				}
			}
		}
		return ordersToReturn;
	}
	
	private String retrieveCorrectMonthsOb(int month, List<DrugOrder> orders, Date startingDate) {
		String drugOrders = "";
		
		if (startingDate != null) {
			Calendar monthDate = Calendar.getInstance();
			monthDate.setTime(startingDate);
			monthDate.add(Calendar.MONTH, month);
			
			Calendar currentDate = Calendar.getInstance();
			
			if (monthDate.before(currentDate)) {
				
				for (DrugOrder current : orders) {
					if (OrderEntryUtil.isCurrent(current, monthDate.getTime())) {
						String drugName = "Drug Missing";
						try {
							drugName = current.getDrug().getName();
						}
						catch (Exception e) {
							System.err.println(e.getMessage());
						}
						
						if (drugOrders.length() > 0) {
							drugOrders = drugOrders + "," + drugName;
						} else {
							drugOrders = drugName;
						}
					}
				}
			}
			
			if (drugOrders.length() > 0) {
				drugOrders = drugOrders + ";" + new SimpleDateFormat("MMM yyyy").format(monthDate.getTime());
			}
		}
		return drugOrders;
	}
	
	private List<Obs> retrieveCorrect6MonthsOb(int month, List<Obs> obs, Date startingDate) {
		List<Obs> obList = new ArrayList<Obs>();
		for (Obs current : obs) {
			int monthsFromStart = calculateMonthsDifference(current.getObsDatetime(), startingDate);
			
			if (monthsFromStart >= month - 3 && monthsFromStart <= month + 3) {
				obList.add(current);
			}
		}
		
		return obList;
	}
	
	private int calculateMonthsDifference(Date observation, Date startingDate) {
		int diff = 0;
		
		Calendar obsDate = Calendar.getInstance();
		obsDate.setTime(observation);
		
		Calendar startDate = Calendar.getInstance();
		startDate.setTime(startingDate);
		
		//find out if there is any difference in years first
		diff = obsDate.get(Calendar.YEAR) - startDate.get(Calendar.YEAR);
		diff = diff * 12;
		
		int monthDiff = obsDate.get(Calendar.MONTH) - startDate.get(Calendar.MONTH);
		diff = diff + monthDiff;
		
		return diff;
	}
	
	private String getDiscontinuedReasons(List<DrugOrder> drugOrderList) {
		String discontinuedReasons = "";
		
		for (DrugOrder o : drugOrderList) {
			if (o.isDiscontinued(null)) {
				if (discontinuedReasons.length() > 0) {
					discontinuedReasons = discontinuedReasons + " , ";
				}
				
				String discontinueReason = OrderEntryUtil.getDiscontinueReason(o);
				if (discontinueReason != null && o.getDrug() != null) {
					discontinuedReasons = discontinuedReasons + o.getDrug().getName() + " - " + discontinueReason;
				}
				
				if (o.getDateStopped() != null) {
					discontinuedReasons = discontinuedReasons + ":"
					        + new SimpleDateFormat("yyyy-MM-dd").format(o.getDateStopped());
				}
			}
		}
		
		return discontinuedReasons;
	}
}

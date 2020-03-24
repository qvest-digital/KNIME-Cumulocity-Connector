package de.tarent.cumulocity.data.alarms;

import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCell;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCellFactory;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.telekom.m2m.cot.restsdk.alarm.Alarm;
import com.telekom.m2m.cot.restsdk.alarm.AlarmApi;
import com.telekom.m2m.cot.restsdk.inventory.ManagedObject;
import com.telekom.m2m.cot.restsdk.util.CotSdkException;

import de.tarent.cumulocity.connector.CotPlatformProvider;
import de.tarent.cumulocity.connector.CumulocityPortObject;

/**
 * This is an example implementation of the node model of the "Alarms" node.
 * 
 * This example node performs simple number formatting
 * ({@link String#format(String, Object...)}) using a user defined format string
 * on all double columns of its input table.
 *
 * @author tarent solutions GmbH
 */
public class CreateAlarmsNodeModel extends NodeModel {

	private static final NodeLogger logger = NodeLogger.getLogger(CreateAlarmsNodeModel.class);
	static final int IN_PORT_DATA_TABLE = 1;

	enum COLUMN_KEYS {
		KEY_ALARM_TYPE("Alarm Type", true), KEY_SEVERITY("Severity", false), KEY_SOURCE_NAME("Source Name", false),
		KEY_SOURCE_ID("Source ID", true), KEY_TEXT("Description", false), KEY_STATUS("Status", false),
		KEY_TIME("Time", false, ZonedDateTimeCellFactory.TYPE);

		private final String m_prettyName;
		public final boolean m_isRequired;
		final DataType m_type;

		COLUMN_KEYS(final String aPrettyName, final boolean aIsRequired, final DataType aType) {
			m_prettyName = aPrettyName;
			m_isRequired = aIsRequired;
			m_type = aType;
		}

		COLUMN_KEYS(final String aPrettyName, final boolean aIsRequired) {
			this(aPrettyName, aIsRequired, StringCell.TYPE);
		}

		@Override
		public String toString() {
			return m_prettyName;
		}
	}

	private enum ALARM_STATUS {
		STATUS_ACTIVE("ACTIVE"),
		STATUS_ACKNOWLEDGED("ACKNOWLEDGED"),
		STATUS_CLEARED("CLEARED");
		
		private final String m_internalName;

		ALARM_STATUS(final String aInternalName) {
			m_internalName = aInternalName;
		}
		@Override
		public String toString() {
			return m_internalName;
		}
		
		public static String toBestMatchingString(final String aInput) {
			for (final ALARM_STATUS v : values()) {
				if (aInput.toLowerCase().contains(v.m_internalName.toLowerCase())) {
					return v.m_internalName;
				}
			}
			return aInput;
		}		
	}
	
	private enum ALARM_SEVERITY {
		SEVERITY_CRITICAL("CRITICAL"),
		SEVERITY_MAJOR("MAJOR"),
		SEVERITY_MINOR("MINOR"),
		SEVERITY_WARNING("WARNING");
		
		private final String m_internalName;

		ALARM_SEVERITY(final String aInternalName) {
			m_internalName = aInternalName;
		}
		@Override
		public String toString() {
			return m_internalName;
		}
		
		public static String toBestMatchingString(final String aInput) {
			for (final ALARM_SEVERITY v : values()) {
				if (aInput.toLowerCase().contains(v.m_internalName.toLowerCase())) {
					return v.m_internalName;
				}
			}
			return aInput;
		}		
	}

	private final SettingsModel[] m_inputColSettings = new SettingsModel[COLUMN_KEYS.values().length];

	/**
	 * @return device id column selection
	 */
	private static SettingsModel createSettingColumn(final String aKey, final DataType aType) {
		return new SettingsModelString(aKey, null);
	}

	/*
	 * we have 2 input port (connection info and alarm fields) and no output port
	 */
	protected CreateAlarmsNodeModel() {
		super(new PortType[] { CumulocityPortObject.TYPE, BufferedDataTable.TYPE }, new PortType[0]);
		int i = 0;

		for (COLUMN_KEYS key : COLUMN_KEYS.values()) {
			m_inputColSettings[i++] = createSettingColumn(key.name(), key.m_type);
		}
	}

	/**
	 * {@inheritDoc}
	 *
	 * @throws CanceledExecutionException - user interrupted the process
	 */
	@Override
	protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec)
			throws CanceledExecutionException {

		final AlarmApi alarmApi = getAlarmApi((CumulocityPortObject) inData[0]);

		final DataTableSpec spec = ((BufferedDataTable) inData[IN_PORT_DATA_TABLE]).getSpec();
		final Map<COLUMN_KEYS, Integer> colIndices = new HashMap<>();
		for (COLUMN_KEYS key : COLUMN_KEYS.values()) {
			final String colName = ((SettingsModelString) m_inputColSettings[key.ordinal()]).getStringValue();
			final int ix = spec.findColumnIndex(colName);
			if (ix > -1) {
				colIndices.put(key, ix);
			}
		}

		final CloseableRowIterator alarmsIterator = ((BufferedDataTable) inData[IN_PORT_DATA_TABLE]).iterator();
		long ctr = 0;
		CotSdkException lastException = null;
		try {
			// iterate over input table rows
			while (alarmsIterator.hasNext()) {

				final DataRow row = alarmsIterator.next();
				final Alarm alarm = new Alarm();

				{
					// indicates the device, that sends the alarm.
					final ManagedObject source = new ManagedObject();
					// Source / Device ID - required field
					source.setId(
							((StringCell) row.getCell(colIndices.get(COLUMN_KEYS.KEY_SOURCE_ID))).getStringValue());
					// Source name - optional
					if (colIndices.containsKey(COLUMN_KEYS.KEY_SOURCE_NAME)) {
						final String sourceName = ((StringCell) row
								.getCell(colIndices.get(COLUMN_KEYS.KEY_SOURCE_NAME))).getStringValue();
						if (notEmpty(sourceName)) {
							source.setName(sourceName);
						}
					}
					alarm.setSource(source);
				}

				// "Alarm ID" is set internally

				// "Alarm Type" - required, note that alarms are aggregated by alarm type and
				// source id!
				final String alarmType = ((StringCell) row.getCell(colIndices.get(COLUMN_KEYS.KEY_ALARM_TYPE))).getStringValue();
				alarm.setType(alarmType);

				// "Description" / "Text" - optional
				if (colIndices.containsKey(COLUMN_KEYS.KEY_TEXT)) {
					final String description = ((StringCell) row.getCell(colIndices.get(COLUMN_KEYS.KEY_TEXT)))
							.getStringValue();
					if (notEmpty(description)) {
						alarm.setText(description);
					}
				}

				// "Creation Time" - is set internally
				// "Time" - optional
				if (colIndices.containsKey(COLUMN_KEYS.KEY_TIME)) {
					final ZonedDateTime time = ((ZonedDateTimeCell) row.getCell(colIndices.get(COLUMN_KEYS.KEY_TIME)))
							.getZonedDateTime();
					if (time != null) {
						alarm.setTime(Date.from(time.toInstant()));
					} else {
						alarm.setTime(new Date());
					}
				} else {
					alarm.setTime(new Date());
				}

				// "First Occurrence Time" - is set internally

				// "Status" - optional
				// check format of alarm status and map to enum if possible
				alarm.setStatus(ALARM_STATUS.STATUS_ACTIVE.toString());
				if (colIndices.containsKey(COLUMN_KEYS.KEY_STATUS)) {
					final String status = ((StringCell) row.getCell(colIndices.get(COLUMN_KEYS.KEY_STATUS)))
							.getStringValue();
					if (notEmpty(status)) {
						// note that status may be any string, but should be one of:
						// STATE_ACKNOWLEDGED, STATE_ACTIVE, STATE_CLEARED (apparently without the 'STATE')
						alarm.setStatus(ALARM_STATUS.toBestMatchingString(status));
					}
				}

				// "Count" - is incremented automatically

				// "Severity" - optional
				// check format of alarm severity and map to enum if possible
				alarm.setSeverity(ALARM_SEVERITY.SEVERITY_WARNING.toString());
				if (colIndices.containsKey(COLUMN_KEYS.KEY_SEVERITY)) {
					// note that severity may be any string, but should be on of:
					// SEVERITY_CRITICAL, SEVERITY_MAJOR, SEVERITY_MINOR, SEVERITY_WARNING (apparently without the 'SEVERITY')
					final String severity = ((StringCell) row.getCell(colIndices.get(COLUMN_KEYS.KEY_SEVERITY)))
							.getStringValue();
					if (notEmpty(severity)) {
						alarm.setSeverity(ALARM_SEVERITY.toBestMatchingString(severity));
					}
				}

				try {
					alarmApi.create(alarm);
					ctr++;
				} catch (CotSdkException cse) {
					logger.error("Failed to write alarm " + alarm + " to Cumulocity!");
					logger.error("Root cause: " + cse.getMessage());
					logger.error("Will continue with other alarms...");
					lastException = cse;
				}
				exec.checkCanceled();
			}
		} catch (CotSdkException cse) {
			logger.error("Failed to write (all) alarms to Cumulocity!");
			logger.error("Root cause: " + cse.getMessage());
			logger.error("Please ensure that you have write permissions to the database!");
			// re-throw
			throw cse;
		} finally {
			if (lastException != null) {
				if (ctr == 0) {
					logger.error("Failed to write any alarms to Cumulocity!");
					logger.error("Root cause: " + lastException.getMessage());
					logger.error("Please ensure that you have write permissions to the database!");
				} else {
					logger.error("Wrote only " + ctr + " of " + ((BufferedDataTable) inData[IN_PORT_DATA_TABLE]).size()
							+ " alarms to Cumulocity.");
					logger.error("Root cause: " + lastException.getMessage());
					logger.error(
							"Please ensure that the device for the given source id (deviced id) exists and that you are permitted to modify them.");
				}
			} else {
				logger.info("Wrote all " + ((BufferedDataTable) inData[IN_PORT_DATA_TABLE]).size()
						+ " alarms to Cumulocity.");
			}
			// not supported alarmsIterator.close();
		}
		return new BufferedDataTable[] {};
	}

	private boolean notEmpty(final String aStr) {
		return aStr != null && aStr.trim().length() > 0;
	}

	private AlarmApi getAlarmApi(final CumulocityPortObject aCoTPortObject) {
		return CotPlatformProvider.getCoTPlatform(getCredentialsProvider(), aCoTPortObject.getConfig()).getAlarmApi();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		final DataTableSpec dataTable = ((DataTableSpec) inSpecs[IN_PORT_DATA_TABLE]);
		// this is only a necessary pre-condition
		int nRequired = 0;

		for (COLUMN_KEYS key : COLUMN_KEYS.values()) {
			if (key.m_isRequired) {
				nRequired++;
			}
		}

		if (dataTable.getNumColumns() < nRequired) {
			throw new InvalidSettingsException("Input table must contain at least " + nRequired + " columns.");
		}
		return new PortObjectSpec[0];

	}

	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
		for (COLUMN_KEYS key : COLUMN_KEYS.values()) {
			m_inputColSettings[key.ordinal()].saveSettingsTo(settings);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		for (COLUMN_KEYS key : COLUMN_KEYS.values()) {
			m_inputColSettings[key.ordinal()].loadSettingsFrom(settings);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		for (COLUMN_KEYS key : COLUMN_KEYS.values()) {
			m_inputColSettings[key.ordinal()].validateSettings(settings);
		}
	}

	@Override
	protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		// Advanced method, usually left empty.
	}

	@Override
	protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		// Advanced method, usually left empty.
	}

	@Override
	protected void reset() {
		// Nothing to do
	}

}

package de.tarent.cumulocity.data.events;

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

import com.telekom.m2m.cot.restsdk.event.Event;
import com.telekom.m2m.cot.restsdk.event.EventApi;
import com.telekom.m2m.cot.restsdk.inventory.ManagedObject;
import com.telekom.m2m.cot.restsdk.util.CotSdkException;

import de.tarent.cumulocity.connector.CotPlatformProvider;
import de.tarent.cumulocity.connector.CumulocityPortObject;

/**
 * implementation of the node model of the "CreateEvents" node.
 * 
 * @author tarent solutions GmbH
 */
public class CreateEventsNodeModel extends NodeModel {

	private static final NodeLogger logger = NodeLogger.getLogger(CreateEventsNodeModel.class);
	static final int IN_PORT_DATA_TABLE = 1;

	enum COLUMN_KEYS {
		KEY_EVENT_TYPE("Event Type", true), KEY_SOURCE_NAME("Source Name", false),
		KEY_SOURCE_ID("Source ID", true), KEY_TEXT("Description", false), 
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

	private final SettingsModel[] m_inputColSettings = new SettingsModel[COLUMN_KEYS.values().length];

	/**
	 * @return device id column selection
	 */
	private static SettingsModel createSettingColumn(final String aKey, final DataType aType) {
		return new SettingsModelString(aKey, null);
	}

	/*
	 * we have 2 input port (connection info and event fields) and no output port
	 */
	protected CreateEventsNodeModel() {
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

		final EventApi eventApi = getEventApi((CumulocityPortObject) inData[0]);

		final DataTableSpec spec = ((BufferedDataTable) inData[IN_PORT_DATA_TABLE]).getSpec();
		final Map<COLUMN_KEYS, Integer> colIndices = new HashMap<>();
		for (COLUMN_KEYS key : COLUMN_KEYS.values()) {
			final String colName = ((SettingsModelString) m_inputColSettings[key.ordinal()]).getStringValue();
			final int ix = spec.findColumnIndex(colName);
			if (ix > -1) {
				colIndices.put(key, ix);
			}
		}

		final CloseableRowIterator eventsIterator = ((BufferedDataTable) inData[IN_PORT_DATA_TABLE]).iterator();
		long ctr = 0;
		CotSdkException lastException = null;
		try {
			// iterate over input table rows
			while (eventsIterator.hasNext()) {

				final DataRow row = eventsIterator.next();
				final Event event = new Event();

				{
					// indicates the device, that sends the event.
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
					event.setSource(source);
				}

				// "Event ID" is set internally

				// "Event Type" - required, use cot_abc_xyz style.
				event.setType(((StringCell) row.getCell(colIndices.get(COLUMN_KEYS.KEY_EVENT_TYPE))).getStringValue());

				// "Description" / "Text" - optional
				if (colIndices.containsKey(COLUMN_KEYS.KEY_TEXT)) {
					final String description = ((StringCell) row.getCell(colIndices.get(COLUMN_KEYS.KEY_TEXT)))
							.getStringValue();
					if (notEmpty(description)) {
						event.setText(description);
					}
				}

				// "Creation Time" - is set internally
				// "Time" - optional
				if (colIndices.containsKey(COLUMN_KEYS.KEY_TIME)) {
					final ZonedDateTime time = ((ZonedDateTimeCell) row.getCell(colIndices.get(COLUMN_KEYS.KEY_TIME)))
							.getZonedDateTime();
					if (time != null) {
						event.setTime(Date.from(time.toInstant()));
					} else {
						event.setTime(new Date());
					}
				} else {
					event.setTime(new Date());
				}

				try {
					eventApi.createEvent(event);
					ctr++;
					if (ctr % 100 == 0) {
						logger.info("Wrote " + ctr + " events to Cumulocity.");						
					}
				} catch (CotSdkException cse) {
					logger.error("Failed to write event " + event + " to Cumulocity!");
					logger.error("Root cause: " + cse.getMessage());
					logger.error("Will continue with other events...");
					lastException = cse;
				}
				exec.checkCanceled();
			}
		} catch (CotSdkException cse) {
			logger.error("Failed to write (all) events to Cumulocity!");
			logger.error("Root cause: " + cse.getMessage());
			logger.error("Please ensure that you have write permissions to the database!");
			// re-throw
			throw cse;
		} finally {
			if (lastException != null) {
				if (ctr == 0) {
					logger.error("Failed to write any events to Cumulocity!");
					logger.error("Root cause: " + lastException.getMessage());
					logger.error("Please ensure that you have write permissions to the database!");
				} else {
					logger.error("Wrote only " + ctr + " of " + ((BufferedDataTable) inData[IN_PORT_DATA_TABLE]).size()
							+ " events to Cumulocity.");
					logger.error("Root cause: " + lastException.getMessage());
					logger.error(
							"Please ensure that the device for the given source id (deviced id) exists and that you are permitted to modify them.");
				}
			} else {
				logger.info("Wrote all " + ((BufferedDataTable) inData[IN_PORT_DATA_TABLE]).size()
						+ " events to Cumulocity.");
			}
		}
		return new BufferedDataTable[] {};
	}

	private EventApi getEventApi(final CumulocityPortObject aCoTPortObject) {
		return CotPlatformProvider.getCoTPlatform(getCredentialsProvider(), aCoTPortObject.getConfig()).getEventApi();
	}

	private boolean notEmpty(final String aStr) {
		return aStr != null && aStr.trim().length() > 0;
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

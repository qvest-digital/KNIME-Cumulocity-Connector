package de.tarent.cumulocity.data.alarms;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.StringValue;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCellFactory;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.google.gson.internal.LazilyParsedNumber;
import com.telekom.m2m.cot.restsdk.alarm.Alarm;
import com.telekom.m2m.cot.restsdk.alarm.AlarmApi;
import com.telekom.m2m.cot.restsdk.util.CotSdkException;
import com.telekom.m2m.cot.restsdk.util.ExtensibleObject;
import com.telekom.m2m.cot.restsdk.util.Filter.FilterBuilder;

import de.tarent.cumulocity.connector.CumulocityPortObject;
import de.tarent.cumulocity.data.IdIterator;
import de.tarent.cumulocity.data.RetrieveDataNodeModel;

/**
 * implementation of the node model of the "Alarms" node.
 * 
 * retrieves alarms from Cumulocity
 * 
 * please note that the code in this class is almost identical to the implementation of EventsNodelModel
 *
 * @author tarent solutions GmbH
 */
public class AlarmsNodeModel extends RetrieveDataNodeModel {

	private static final NodeLogger logger = NodeLogger.getLogger(AlarmsNodeModel.class);
	private static final int IN_PORT_CONNECTION_SETTINGS = 0;

	private static final int RESULT_SIZE = 100;

	/*
	 * we have 1 required and one optional input port (connection info + device
	 * selection) and one output port with the alarms
	 */
	protected AlarmsNodeModel() {
		super(new PortType[] { CumulocityPortObject.TYPE, BufferedDataTable.TYPE_OPTIONAL });
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		if (inSpecs == null || inSpecs.length == 0) {
			throw new InvalidSettingsException(
					"Cumulocity Connection Info required on port " + IN_PORT_CONNECTION_SETTINGS);
		}
		if (inSpecs.length == 2) {
			boolean hasStringColumn = false;
			final DataTableSpec dataTable = ((DataTableSpec) inSpecs[RetrieveDataNodeModel.IN_PORT_DATA_TABLE]);
			if (dataTable != null) {
				for (int i = 0; (i < dataTable.getNumColumns()) && !hasStringColumn; i++) {
					final DataColumnSpec columnSpec = dataTable.getColumnSpec(i);
					if (columnSpec.getType().isCompatible(StringValue.class)) {
						// found one string column
						hasStringColumn = true;
					}
				}
				if (!hasStringColumn) {
					throw new InvalidSettingsException("Input table must contain at least one String column");
				}
			}
		}
		return super.configure(inSpecs);
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
		
		final long maxNum = getMaxNumItemsToFetch();

		final IdIterator device_ids = retrieveDeviceIDs((BufferedDataTable) inData[IN_PORT_DATA_TABLE]);

		long rowIx = 0;
		final DataTableSpec outputSpec = outputTableSpec();
		final BufferedDataContainer container = exec.createDataContainer(outputSpec);

		while (rowIx < maxNum && device_ids.hasNext()) {
			final Optional<FilterBuilder> optionalFilter = addOptionalDateFilter(device_ids.next());
			final Iterator<Alarm> alarmsIterator;
			if (optionalFilter.isPresent()) {
				alarmsIterator = alarmApi.getAlarms(optionalFilter.get(), RESULT_SIZE).stream().iterator();
			} else {
				alarmsIterator = alarmApi.getAlarms(RESULT_SIZE).stream().iterator();
			}

			rowIx = retrieveAlarmsForFilter(container, exec, maxNum, alarmsIterator, rowIx);
		}

		final BufferedDataTable out = container.getTable();
		return new BufferedDataTable[] { out };
	}

	protected long retrieveAlarmsForFilter(final BufferedDataContainer container, 
			final ExecutionContext exec, final long maxNum,
			final Iterator<Alarm> alarmsIterator, long rowIx) throws CanceledExecutionException {
		final DataCell[] cells = new DataCell[11];
		try {
			// Create output table specification
			while (alarmsIterator.hasNext()) {
				final Alarm alarm = alarmsIterator.next();
				final Map<String, Object> attributes = alarm.getAttributes();
				cells[0] = new StringCell(alarm.getId());
				cells[1] = new StringCell(alarm.getType());
				cells[2] = new StringCell(alarm.getSeverity());
				final Date creationtime = alarm.getCreationTime();
				cells[3] = ZonedDateTimeCellFactory.create(m_dateFormat.format(creationtime));
				final LazilyParsedNumber count_number = (LazilyParsedNumber) alarm.get("count");
				if (count_number instanceof LazilyParsedNumber) {
					cells[4] = new IntCell(count_number.intValue());
				} else {
					cells[4] = DataType.getMissingCell();
				}
				final Object source = attributes.get("source");
				if (source != null && source instanceof ExtensibleObject) {
					String source_name = (String) ((ExtensibleObject) source).get("name");
					cells[5] = new StringCell(source_name);
					String source_id = (String) ((ExtensibleObject) source).get("id");
					cells[6] = new StringCell(source_id);
				}
				cells[7] = new StringCell(alarm.getText());
				cells[8] = new StringCell(alarm.getStatus());
				final Date time = (Date) attributes.get("time");
				cells[9] = ZonedDateTimeCellFactory.create(m_dateFormat.format(time));
				final Date occurance_date = (Date) attributes.get("firstOccurrenceTime");
				if (occurance_date == null) {
					cells[10] = DataType.getMissingCell();
				} else {
					cells[10] = ZonedDateTimeCellFactory.create(m_dateFormat.format(occurance_date));
				}
				final RowKey key = RowKey.createRowKey(rowIx);
				final DataRow row = new DefaultRow(key, cells);
				container.addRowToTable(row);
				rowIx++;
				if (rowIx >= maxNum) {
					logger.info("Retrieved maximal number (" + rowIx + ") of alarms to retrieve, will stop.");
					break;
				}
				exec.checkCanceled();
			}
		} catch (CotSdkException cse) {
			if (rowIx == 0) {
				logger.error("Failed to retrieve any alarms!");
				throw cse;
			} else {
				logger.error("Failed to retrieve all alarms!");
			}
			logger.error("Root cause: " + cse.getMessage());
		} finally {
			container.close();
			// not supported alarmsIterator.close();
		}
		return rowIx;
	}

	protected DataTableSpec outputTableSpec() {
		final List<DataColumnSpec> columns = new ArrayList<>();
		columns.add(new DataColumnSpecCreator("Alarm ID", StringCell.TYPE).createSpec());
		columns.add(new DataColumnSpecCreator("Alarm Type", StringCell.TYPE).createSpec());
		columns.add(new DataColumnSpecCreator("Severity", StringCell.TYPE).createSpec());
		columns.add(new DataColumnSpecCreator("Creation Time", ZonedDateTimeCellFactory.TYPE).createSpec());
		columns.add(new DataColumnSpecCreator("Count", IntCell.TYPE).createSpec());
		columns.add(new DataColumnSpecCreator("Source Name", StringCell.TYPE).createSpec());
		columns.add(new DataColumnSpecCreator("Source ID", StringCell.TYPE).createSpec());
		columns.add(new DataColumnSpecCreator("Description", StringCell.TYPE).createSpec());
		columns.add(new DataColumnSpecCreator("Status", StringCell.TYPE).createSpec());
		columns.add(new DataColumnSpecCreator("Time", ZonedDateTimeCellFactory.TYPE).createSpec());
		columns.add(new DataColumnSpecCreator("First Occurrence Time", ZonedDateTimeCellFactory.TYPE).createSpec());
		final DataTableSpec outputSpec = new DataTableSpec(columns.toArray(new DataColumnSpec[0]));
		return outputSpec;
	}

}

package de.tarent.cumulocity.data.measurements;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.knime.core.data.def.DoubleCell;
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
import com.telekom.m2m.cot.restsdk.measurement.Measurement;
import com.telekom.m2m.cot.restsdk.measurement.MeasurementApi;
import com.telekom.m2m.cot.restsdk.measurement.MeasurementCollection;
import com.telekom.m2m.cot.restsdk.util.ExtensibleObject;
import com.telekom.m2m.cot.restsdk.util.Filter.FilterBuilder;

import de.tarent.cumulocity.connector.CumulocityPortObject;
import de.tarent.cumulocity.data.IdIterator;
import de.tarent.cumulocity.data.RetrieveDataNodeModel;

/**
 * @author tarent solutions GmbH
 */
public class MeasurementsNodeModel extends RetrieveDataNodeModel {

	private static final NodeLogger logger = NodeLogger.getLogger(MeasurementsNodeModel.class);


	/*
	 * we have 2 input ports (connection info and device info) and one output port
	 * with the measurements
	 */
	protected MeasurementsNodeModel() {
		super(new PortType[] { CumulocityPortObject.TYPE, BufferedDataTable.TYPE });
	}

	/**
	 * {@inheritDoc}
	 *
	 * @throws CanceledExecutionException - user interrupted the process
	 */
	@Override
	protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec)
			throws CanceledExecutionException {

		final MeasurementApi measurementApi = getMeasurementApi((CumulocityPortObject) inData[0]);
		final IdIterator device_ids = retrieveDeviceIDs((BufferedDataTable) inData[IN_PORT_DATA_TABLE]);

		long nIgnored = 0;
		long rowIx = 0;
		final long maxNum = getMaxNumItemsToFetch();

		final BufferedDataContainer container = exec.createDataContainer(outputTableSpec());
		// can be re-used as addRowToTable copies its content
		final DataCell[] cells = new DataCell[8];
		try {
			while (device_ids.hasNext() && rowIx < maxNum) {
				// Retrieve measurement collection API
				final Iterator<Measurement> mcol = getMeasurementCollectionApi(measurementApi, device_ids.next())
						.stream().iterator();

				while (mcol.hasNext() && rowIx < maxNum) {
					final Measurement measurement = mcol.next();
					final StringCell idCell = new StringCell(measurement.getId());
					final DataCell typeCell;
					if (measurement.getType() == null) {
						typeCell = DataType.getMissingCell();
					} else {
						typeCell = new StringCell(measurement.getType());
					}

					final Map<String, Object> attributes = measurement.getAttributes();

					final Object source = attributes.get("source");
					final DataCell sourceCell;
					if ((source != null) && (source instanceof ExtensibleObject)) {
						final String source_id = ((ExtensibleObject) source).get("id").toString();
						sourceCell = new StringCell(source_id);
					} else {
						sourceCell = DataType.getMissingCell();
					}
					final Date measurement_Date = measurement.getTime();
					final DataCell dateCell = ZonedDateTimeCellFactory.create(m_dateFormat.format(measurement_Date));
					// these values stay the same for all measurements for this device
					cells[0] = idCell;
					cells[1] = typeCell;
					cells[2] = sourceCell;
					cells[3] = dateCell;

					for (final Entry<String, Object> entry : attributes.entrySet()) {
						if ((entry.getValue() != null) && (entry.getValue() instanceof ExtensibleObject)) {
							final Map<String, Object> frag_attributes = ((ExtensibleObject) entry.getValue())
									.getAttributes();
							//"Measurement Subtype"
							cells[4] = new StringCell(entry.getKey());
							boolean useful = false;
							for (final Entry<String, Object> frag_attr : frag_attributes.entrySet()) {
								// The row id should be set to work with multi dimensional fragments,
								// measurements greater than 2000 and measurements for different devices.

								final Object req_fragment = frag_attr.getValue();
								if ((req_fragment != null) && (req_fragment instanceof ExtensibleObject)) {
									//"Fragment Series"
									cells[5] = new StringCell(frag_attr.getKey());
									final ExtensibleObject subElem = ((ExtensibleObject) req_fragment);

									final Object number = subElem.get("value");
									if (number instanceof LazilyParsedNumber) {
										final Double measurement_value = ((LazilyParsedNumber) number).doubleValue();
										cells[6] = new DoubleCell(measurement_value);
									} else {
										cells[6] = DataType.getMissingCell();
									}

									if (subElem.has("unit")) {
										final String measurement_unit = subElem.get("unit").toString();
										cells[7] = new StringCell(measurement_unit);
									} else {
										cells[7] = DataType.getMissingCell();
									}

									final RowKey key = RowKey.createRowKey(rowIx);
									final DataRow row = new DefaultRow(key, cells);
									rowIx++;
									useful = true;
									container.addRowToTable(row);
								}
								exec.checkCanceled();
							}
							if (!useful) {
								logger.info("Ignoring empty measurement: " + measurement.getId());
								nIgnored++;
								//measurementApi.delete(measurement);
							}
						}
					}
					if (rowIx >= maxNum) {
						logger.info("Retrieved maximal number (" + rowIx + ") of measurements to retrieve, will stop.");
						break;
					}
				}
			}
		} finally {
			container.close();
			device_ids.close();
		}
		if (nIgnored > 0) {
			logger.info("Ignored " + nIgnored + " measurements.");
		}
		return new BufferedDataTable[] { container.getTable() };
	}

	protected DataTableSpec outputTableSpec() {
		final List<DataColumnSpec> columns = new ArrayList<>();
		columns.add(new DataColumnSpecCreator("Measurement ID", StringCell.TYPE).createSpec());
		columns.add(new DataColumnSpecCreator("Measurement Type", StringCell.TYPE).createSpec());
		columns.add(new DataColumnSpecCreator("Device ID", StringCell.TYPE).createSpec());
		columns.add(new DataColumnSpecCreator("Zoned Date Time", ZonedDateTimeCellFactory.TYPE).createSpec());
		columns.add(new DataColumnSpecCreator("Measurement Subtype", StringCell.TYPE).createSpec());
		columns.add(new DataColumnSpecCreator("Fragment Series", StringCell.TYPE).createSpec());
		columns.add(new DataColumnSpecCreator("Value", DoubleCell.TYPE).createSpec());
		columns.add(new DataColumnSpecCreator("Unit", StringCell.TYPE).createSpec());
		final DataTableSpec outputSpec = new DataTableSpec(columns.toArray(new DataColumnSpec[0]));
		return outputSpec;
	}

	private MeasurementCollection getMeasurementCollectionApi(final MeasurementApi aApi, 
			final Optional<FilterBuilder> aDeviceIdFilter) {
		final MeasurementCollection measurementCollection;
		// final FilterBuilder filter = Filter.build().setFilter(FilterBy.BYSOURCE, aDeviceId);

		// doesn't work as we don't know the expected date format....
//		if (dateRestrictions.getFirst().isPresent()) {
//			filter.setFilter(FilterBy.BYDATEFROM, m_c8yDateFormat.format(dateRestrictions.getFirst().get()));
//		}
//		if (dateRestrictions.getSecond().isPresent()) {
//			filter.setFilter(FilterBy.BYDATETO, m_c8yDateFormat.format(dateRestrictions.getSecond().get()));
//		}

		// size of the results (Max. 2000)
		measurementCollection = aApi.getMeasurements(addOptionalDateFilter(aDeviceIdFilter).get(), 2000);
		return measurementCollection;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		boolean hasStringColumn = false;
		final DataTableSpec dataTable = ((DataTableSpec) inSpecs[IN_PORT_DATA_TABLE]);
		// this is only a necessary pre-condition
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
		return super.configure(inSpecs);

	}
}

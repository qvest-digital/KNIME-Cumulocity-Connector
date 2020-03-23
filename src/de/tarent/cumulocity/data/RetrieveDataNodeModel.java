package de.tarent.cumulocity.data;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.TimeZone;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelDate;
import org.knime.core.node.defaultnodesettings.SettingsModelLong;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.util.Pair;

import com.telekom.m2m.cot.restsdk.alarm.AlarmApi;
import com.telekom.m2m.cot.restsdk.event.EventApi;
import com.telekom.m2m.cot.restsdk.measurement.MeasurementApi;
import com.telekom.m2m.cot.restsdk.measurement.MeasurementCollection;
import com.telekom.m2m.cot.restsdk.util.Filter;
import com.telekom.m2m.cot.restsdk.util.Filter.FilterBuilder;
import com.telekom.m2m.cot.restsdk.util.FilterBy;

import de.tarent.cumulocity.connector.CotPlatformProvider;
import de.tarent.cumulocity.connector.CumulocityPortObject;

/**
 * @author tarent (vlahar)
 */
public abstract class RetrieveDataNodeModel extends NodeModel {

	static final int IN_PORT_DATA_TABLE = 1;

	private static final String KEY_DEVICE_ID_COLUMN = "key_device_id_col";

	// private static final NodeLogger logger =
	// NodeLogger.getLogger(GetMeasurementsNodeModel.class);
	/**
	 * the settings key which is used to retrieve and store the settings (from the
	 * dialog or from a settings file) (package visibility to be usable from the
	 * dialog).
	 */
	static final String Config_From_Date = "From_Date";
	static final String Config_To_Date = "To_Date";
	static final String Config_def_Date = "Def_Date";
	static final String Config_MAX_NUM_RECORDS = "MAX_NUM_RECORDS";
	private final SettingsModelDate m_fromDateSettings = createDateSettings(Config_From_Date);
	private final SettingsModelDate m_toDateSettings = createDateSettings(Config_To_Date);
	protected final SettingsModelLong m_maxNumRecordsSettings = createLongSettings(Config_MAX_NUM_RECORDS, -1);
	private final Date m_earliestDate = new Date(0);

	protected final SimpleDateFormat m_dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX[z]");

	final static SettingsModelDate createDateSettings(final String aLabel) {
		return new SettingsModelDate(aLabel);
	}

	final static SettingsModelLong createLongSettings(final String aLabel, final long aDefault) {
		return new SettingsModelLong(aLabel, aDefault);
	}

	/*
	 * we have 1 or 2 input ports (connection info and optionally device info) and
	 * one output port with the measurements/alarms/events
	 */
	protected RetrieveDataNodeModel(final PortType[] aInputPorts) {
		super(aInputPorts, new PortType[] { BufferedDataTable.TYPE });
		m_dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	protected Pair<Optional<Date>, Optional<Date>> getFromTo() {
		return getFromTo(m_fromDateSettings, m_toDateSettings);
	}

	protected Pair<Optional<Date>, Optional<Date>> getFromTo(final SettingsModelDate aFromSettings,
			final SettingsModelDate aToSettings) {
		final boolean fromIsEnabled = aFromSettings.getSelectedFields() != 0;
		final boolean toIsEnabled = aToSettings.getSelectedFields() != 0;
		final Optional<Date> from;
		if (fromIsEnabled) {
			from = Optional.of(aFromSettings.getDate());
		} else {
			from = Optional.empty();
		}
		final Optional<Date> to;
		if (toIsEnabled) {
			to = Optional.of(aToSettings.getDate());
		} else {
			to = Optional.empty();
		}
		return new Pair<>(from, to);
	}

	protected MeasurementCollection getMeasurementCollectionApi(final MeasurementApi aApi, final String aDeviceId) {
		final MeasurementCollection measurementCollection;
		final FilterBuilder filter = Filter.build().setFilter(FilterBy.BYSOURCE, aDeviceId);

		// doesn't work as we don't know the expected date format....
//		if (dateRestrictions.getFirst().isPresent()) {
//			filter.setFilter(FilterBy.BYDATEFROM, m_c8yDateFormat.format(dateRestrictions.getFirst().get()));
//		}
//		if (dateRestrictions.getSecond().isPresent()) {
//			filter.setFilter(FilterBy.BYDATETO, m_c8yDateFormat.format(dateRestrictions.getSecond().get()));
//		}

		// size of the results (Max. 2000)
		measurementCollection = aApi.getMeasurements(addOptionalDateFilter(Optional.of(filter)).get(), 2000);
		return measurementCollection;
	}
	
	protected Optional<FilterBuilder> getOptionalDateFilter() {
		return addOptionalDateFilter(Optional.empty());
	}
	
	private Optional<FilterBuilder> addOptionalDateFilter(final Optional<FilterBuilder> aFilterOption) {
		// If From and To set to default, call the API with only Device ID
		final Pair<Optional<Date>, Optional<Date>> dateRestrictions = getFromTo();
		if (dateRestrictions.getFirst().isPresent() || dateRestrictions.getSecond().isPresent()) {
			final FilterBuilder filter;
			if (aFilterOption.isPresent()) {
				filter = aFilterOption.get();
			} else {
				filter = Filter.build();
			}
			return Optional.of(filter.byDate(dateRestrictions.getFirst().orElse(m_earliestDate),
					dateRestrictions.getSecond().orElse(new Date(System.currentTimeMillis() + 100000))));
		} else {
			if (aFilterOption.isPresent()) {
				return aFilterOption;
			} else {
				return Optional.empty();
			}
		}
	}

	protected MeasurementApi getMeasurementApi(final CumulocityPortObject aCoTPortObject) {
		return CotPlatformProvider.getCoTPlatform(getCredentialsProvider(), aCoTPortObject.getConfig())
				.getMeasurementApi();
	}

	protected AlarmApi getAlarmApi(final CumulocityPortObject aCoTPortObject) {
		return CotPlatformProvider.getCoTPlatform(getCredentialsProvider(), aCoTPortObject.getConfig()).getAlarmApi();
	}

	protected EventApi getEventApi(final CumulocityPortObject aCoTPortObject) {
		return CotPlatformProvider.getCoTPlatform(getCredentialsProvider(), aCoTPortObject.getConfig()).getEventApi();
	}

	protected abstract DataTableSpec outputTableSpec();

	@Override
	protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		return new PortObjectSpec[] { outputTableSpec() };
	}

	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
		m_fromDateSettings.saveSettingsTo(settings);
		m_toDateSettings.saveSettingsTo(settings);
		m_maxNumRecordsSettings.saveSettingsTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_fromDateSettings.loadSettingsFrom(settings);
		m_toDateSettings.loadSettingsFrom(settings);
		m_maxNumRecordsSettings.loadSettingsFrom(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_fromDateSettings.validateSettings(settings);
		m_toDateSettings.validateSettings(settings);
		m_maxNumRecordsSettings.validateSettings(settings);

		final SettingsModelDate fromDateSettings = createDateSettings(Config_From_Date);
		fromDateSettings.loadSettingsFrom(settings);
		final SettingsModelDate toDateSettings = createDateSettings(Config_To_Date);
		toDateSettings.loadSettingsFrom(settings);

		final Pair<Optional<Date>, Optional<Date>> dateRestrictions = getFromTo(fromDateSettings, toDateSettings);
		if (dateRestrictions.getFirst().isPresent() && dateRestrictions.getSecond().isPresent()) {
			if ((dateRestrictions.getFirst().get().compareTo(dateRestrictions.getSecond().get()) > 0)) {
				throw new InvalidSettingsException("Entered '" + RetrieveDataNodeDialog.FROM_DATE_LABEL + "' is after '"
						+ RetrieveDataNodeDialog.TO_DATE_LABEL + "': not valid");
			}
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

	/**
	 * @return device id column selection
	 */
	protected static SettingsModelString createSettingsDeviceIdColumn() {
		return new SettingsModelString(KEY_DEVICE_ID_COLUMN, null);
	}
}

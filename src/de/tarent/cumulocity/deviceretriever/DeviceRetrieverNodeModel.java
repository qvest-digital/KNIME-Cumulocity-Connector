package de.tarent.cumulocity.deviceretriever;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.telekom.m2m.cot.restsdk.CloudOfThingsPlatform;
import com.telekom.m2m.cot.restsdk.inventory.InventoryApi;
import com.telekom.m2m.cot.restsdk.inventory.ManagedObject;
import com.telekom.m2m.cot.restsdk.inventory.ManagedObjectCollection;

import de.tarent.cumulocity.connector.CotPlatformProvider;
import de.tarent.cumulocity.connector.CumulocityPortObject;

/**
 * <code>NodeModel</code> for the "CumulocityConnectorDeviceRetriever" node.
 *
 * @author tarent solutions GmbH (vlahar)
 */
public class DeviceRetrieverNodeModel extends NodeModel {

	private static final String DEVICE_NAME = "Device Name";

	private static final String TYPE = "Type";

	private static final String DEVICE_ID = "Device ID";

	private static final NodeLogger logger = NodeLogger.getLogger(DeviceRetrieverNodeModel.class);

	private static final int DEVICE_ID_POS = 0;

	private static final int TYPE_POS = 1;

	private static final int DEVICE_NAME_POS = 2;

	/**
	 * Constructor for the node model. There is one input port with the cumulocity
	 * connection and out output port for the data table with the device information
	 */
	public DeviceRetrieverNodeModel() {
		super(new PortType[] { CumulocityPortObject.TYPE }, new PortType[] { BufferedDataTable.TYPE });
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws CanceledExecutionException - user interrupted the process
	 */
	@Override
	protected PortObject[] execute(final PortObject[] inPort, final ExecutionContext exec)
			throws CanceledExecutionException {
		final CumulocityPortObject cotPortObject = (CumulocityPortObject) inPort[0];
		final BufferedDataContainer container = fillDeviceTable(exec,
				CotPlatformProvider.getCoTPlatform(getCredentialsProvider(), cotPortObject.getConfig()));

		return new PortObject[] { container.getTable() };
	}

	private BufferedDataContainer fillDeviceTable(final ExecutionContext exec, final CloudOfThingsPlatform platform)
			throws CanceledExecutionException {
		final ManagedObjectCollection managedObjectCollection = getManagedObjects(platform);
		final DataTableSpec outputSpec = getOutputTableSpec();

		final BufferedDataContainer container = exec.createDataContainer(outputSpec);
		try (final Stream<ManagedObject> managedObjectsStream = managedObjectCollection.stream()) {
			long rowIx = 0;
			Iterator<ManagedObject> iter = managedObjectsStream.iterator();
			while (iter.hasNext()) {
				final ManagedObject managedObject = iter.next();
				final RowKey key = RowKey.createRowKey(rowIx++);
				final DataCell[] cells = new DataCell[3];
				cells[DEVICE_ID_POS] = new StringCell(managedObject.getId());
				if (managedObject.getType() == null) {
					cells[TYPE_POS] = DataType.getMissingCell();
				} else {
					cells[TYPE_POS] = new StringCell(managedObject.getType());
				}
				if (managedObject.getName() == null) {
					cells[DEVICE_NAME_POS] = DataType.getMissingCell();
				} else {
					cells[DEVICE_NAME_POS] = new StringCell(managedObject.getName());
				}
				final DataRow row = new DefaultRow(key, cells);
				container.addRowToTable(row);
				exec.checkCanceled();
			}
		}

		container.close();
		logger.infoWithFormat("Read data of %d devices.", container.size());
		return container;
	}

	/**
	 * 
	 * @return DataTableSpec with three pre-defined (and fixed) columns: device id,
	 *         type and name
	 */
	private DataTableSpec getOutputTableSpec() {
		final DataColumnSpec[] allColSpecs = new DataColumnSpec[3];

		allColSpecs[DEVICE_ID_POS] = new DataColumnSpecCreator(DEVICE_ID, StringCell.TYPE).createSpec();
		allColSpecs[TYPE_POS] = new DataColumnSpecCreator(TYPE, StringCell.TYPE).createSpec();
		allColSpecs[DEVICE_NAME_POS] = new DataColumnSpecCreator(DEVICE_NAME, StringCell.TYPE).createSpec();
		return new DataTableSpec(allColSpecs);
	}

	private ManagedObjectCollection getManagedObjects(final CloudOfThingsPlatform platform) {
		final InventoryApi inventoryApi = platform.getInventoryApi();
		// TODO - make page size a configurable parameter ?
		return inventoryApi.getManagedObjects(1000);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void reset() {
		// nothing to do
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) {
		// input ports are no relevant
		return new PortObjectSpec[] { getOutputTableSpec() };
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadInternals(final File internDir, final ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		// nothing to do
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveInternals(final File internDir, final ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		// nothing to do
	}

}

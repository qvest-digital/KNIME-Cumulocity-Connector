package de.tarent.cumulocity.data;

import org.knime.core.data.StringValue;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentDate;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.util.DataValueColumnFilter;

/**
 * @author tarent solutions GmbH
 */
public class RetrieveDataNodeDialog extends DefaultNodeSettingsPane {

	static final String TO_DATE_LABEL = "To Date";
	static final String FROM_DATE_LABEL = "From Date";
	static final String MAX_NUM_RECORDS_LABEL = "Max number of records to retrieve";

	public RetrieveDataNodeDialog(final boolean aAddDeviceIdCol, final int aLimitNumRecords, 
			final boolean aRequireDeviceId) {
		super();

		if (aAddDeviceIdCol) {
			// option for device id column
			@SuppressWarnings("unchecked")
			final DialogComponentColumnNameSelection deviceIdCol = new DialogComponentColumnNameSelection(
					RetrieveDataNodeModel.createSettingsDeviceIdColumn(), "Device IDs",
					RetrieveDataNodeModel.IN_PORT_DATA_TABLE, aRequireDeviceId, !aRequireDeviceId,
					new DataValueColumnFilter(StringValue.class));
			this.addDialogComponent(deviceIdCol);
		}
		if (aLimitNumRecords > 0) {
			addDialogComponent(new DialogComponentNumber(
					RetrieveDataNodeModel.createLongSettings(RetrieveDataNodeModel.Config_MAX_NUM_RECORDS, aLimitNumRecords),
					MAX_NUM_RECORDS_LABEL, 100));
		}

		// users may optionally use date restrictions to restrict the number of
		// measurements to retrieve
		addDialogComponent(new DialogComponentDate(
				RetrieveDataNodeModel.createDateSettings(RetrieveDataNodeModel.Config_From_Date), FROM_DATE_LABEL));
		addDialogComponent(new DialogComponentDate(
				RetrieveDataNodeModel.createDateSettings(RetrieveDataNodeModel.Config_To_Date), TO_DATE_LABEL));
	}
}

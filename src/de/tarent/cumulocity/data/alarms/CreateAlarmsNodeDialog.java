package de.tarent.cumulocity.data.alarms;

import org.knime.core.data.DataValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.StringValue;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCellFactory;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeValue;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.DataValueColumnFilter;

import de.tarent.cumulocity.data.alarms.CreateAlarmsNodeModel.COLUMN_KEYS;

/**
 * implementation of the node dialog of the "CreateAlarms" node.
 * 
 * @author tarent solutions GmbH
 */
public class CreateAlarmsNodeDialog extends DefaultNodeSettingsPane {

	/**
	 * New dialog pane for configuring the node. The dialog created here will show
	 * up when double clicking on a node in KNIME Analytics Platform.
	 */
	CreateAlarmsNodeDialog() {
		super();
		for (COLUMN_KEYS key : COLUMN_KEYS.values()) {
			final SettingsModelString model = new SettingsModelString(key.name(), key.toString());
			final Class<? extends DataValue> cellClass;
			if (key.m_type == StringCell.TYPE) {
				cellClass = StringValue.class;
			} else if (key.m_type == IntCell.TYPE) {
				cellClass = IntValue.class;
			} else if (key.m_type == ZonedDateTimeCellFactory.TYPE) {
				cellClass = ZonedDateTimeValue.class;
			} else {
				cellClass = null;
			}
			if (cellClass != null) {
				final DialogComponent col = new DialogComponentColumnNameSelection(model, key.toString(),
						CreateAlarmsNodeModel.IN_PORT_DATA_TABLE, key.m_isRequired, !key.m_isRequired,
						new DataValueColumnFilter(cellClass));
				this.addDialogComponent(col);
			}
		}
	}
}

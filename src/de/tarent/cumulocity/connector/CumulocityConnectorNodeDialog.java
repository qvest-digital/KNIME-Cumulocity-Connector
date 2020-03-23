package de.tarent.cumulocity.connector;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentAuthentication;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;

/**
 * <code>NodeDialog</code> for the "CumulocityConnector" node.
 *
 * @author tarent solutions GmbH (vlahar)
 */
public class CumulocityConnectorNodeDialog extends DefaultNodeSettingsPane {

	/* Panel and model for authentication. */
	private final SettingsModelAuthentication m_authenticationModel;

	private final DialogComponentAuthentication m_authenticationPanel;

	/**
	 * New pane for configuring the CumulocityConnector node.
	 */
	protected CumulocityConnectorNodeDialog() {

		addDialogComponent(new DialogComponentString(CumulocityConnectorNodeModel.settingsModelUrl(),
				"Host", true, 30));
		addDialogComponent(new DialogComponentString(CumulocityConnectorNodeModel.settingsModelTenant(),
				"Tenant", false, 10));
		
		m_authenticationModel = CumulocityConnectorNodeModel.settingsModelAuthentication();

		m_authenticationPanel = new DialogComponentAuthentication(m_authenticationModel, null,
				AuthenticationType.USER_PWD, AuthenticationType.CREDENTIALS);

		addDialogComponent(m_authenticationPanel);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
		m_authenticationModel.saveSettingsTo(settings);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void loadAdditionalSettingsFrom(final NodeSettingsRO settings,
            final DataTableSpec[] specs) throws NotConfigurableException {
		try {
			m_authenticationModel.loadSettingsFrom(settings);

			m_authenticationPanel.loadSettingsFrom(settings, specs, getCredentialsProvider());
		} catch (InvalidSettingsException e) {
			throw new NotConfigurableException(e.getMessage(), e);
		}
    }
}


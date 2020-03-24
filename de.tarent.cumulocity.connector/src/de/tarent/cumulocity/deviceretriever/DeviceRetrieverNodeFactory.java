package de.tarent.cumulocity.deviceretriever;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * This is the implementation of the node factory of the
 * "CumulocityConnectorDeviceRetriever" node.
 *
 * @author tarent solutions GmbH (vlahar)
 */
public class DeviceRetrieverNodeFactory
		extends NodeFactory<DeviceRetrieverNodeModel> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DeviceRetrieverNodeModel createNodeModel() {
		// Create and return a new node model.
		return new DeviceRetrieverNodeModel();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNrNodeViews() {
		// The number of views the node should have, in this cases there is none.
		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NodeView<DeviceRetrieverNodeModel> createNodeView(final int viewIndex,
			final DeviceRetrieverNodeModel nodeModel) {
		// We return null as there is no view
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasDialog() {
		// Indication whether the node has a dialog or not.
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NodeDialogPane createNodeDialogPane() {
		return new DeviceRetrieverNodeDialog();
	}

}

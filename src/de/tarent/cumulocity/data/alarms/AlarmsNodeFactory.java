package de.tarent.cumulocity.data.alarms;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

import de.tarent.cumulocity.data.RetrieveDataNodeDialog;

/**
 * simple implementation of the node factory of the
 * "Alarms" node.
 *
 * @author tarent solutions GmbH
 */
public class AlarmsNodeFactory 
        extends NodeFactory<AlarmsNodeModel> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public AlarmsNodeModel createNodeModel() {
		return new AlarmsNodeModel();
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
	public NodeView<AlarmsNodeModel> createNodeView(final int viewIndex,
			final AlarmsNodeModel nodeModel) {
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasDialog() {
		// Indication whether the node has a dialog or not.
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NodeDialogPane createNodeDialogPane() {
		return new RetrieveDataNodeDialog(false, 1000);
	}

}


package de.tarent.cumulocity.data.events;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

import de.tarent.cumulocity.data.RetrieveDataNodeDialog;

/**
 * simple implementation of the node factory of the
 * Cumulocity "Events" node.
 *
 * @author tarent solutions GmbH
 */
public class EventsNodeFactory 
        extends NodeFactory<EventsNodeModel> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public EventsNodeModel createNodeModel() {
		return new EventsNodeModel();
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
	public NodeView<EventsNodeModel> createNodeView(final int viewIndex,
			final EventsNodeModel nodeModel) {
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
		return new RetrieveDataNodeDialog(false, 100);
	}

}


package de.tarent.cumulocity.data.alarms;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * implementation of the node factory of the
 * "CreateAlarms" node.
 *
 * @author tarent solutions GmbH
 */
public class CreateAlarmsNodeFactory 
        extends NodeFactory<CreateAlarmsNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public CreateAlarmsNodeModel createNodeModel() {
		// Create and return a new node model.
        return new CreateAlarmsNodeModel();
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
    public NodeView<CreateAlarmsNodeModel> createNodeView(final int viewIndex,
            final CreateAlarmsNodeModel nodeModel) {
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
        return new CreateAlarmsNodeDialog();
    }

}


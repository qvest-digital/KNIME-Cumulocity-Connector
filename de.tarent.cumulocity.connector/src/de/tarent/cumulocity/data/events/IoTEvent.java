package de.tarent.cumulocity.data.events;

import java.io.Serializable;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCellFactory;

import com.google.gson.JsonElement;

/**
 * helper class to de-serialize events received from Cumulocity
 * 
 * @author tarent solutions GmbH
 *
 */
public class IoTEvent implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public final DataCell m_eventId;
	public final DataCell m_eventType;
	public final DataCell m_creationTime;
	public final DataCell m_sourceName;
	public final DataCell m_sourceId;
	public final DataCell m_time;
	public final DataCell m_description;

	public IoTEvent(final JsonElement eventId, final JsonElement jsonElement, final JsonElement creationTime,
			final JsonElement sourceName, final JsonElement sourceId, final JsonElement eventTime,
			final JsonElement description) {
		m_eventId = setStringCellValue(eventId);
		m_eventType = setStringCellValue(jsonElement);

		if (creationTime != null) {
			m_creationTime = ZonedDateTimeCellFactory.create(creationTime.getAsString());
		} else {
			m_creationTime = DataType.getMissingCell();
		}

		m_sourceName = setStringCellValue(sourceName);
		m_sourceId = setStringCellValue(sourceId);

		if (eventTime != null) {
			m_time = ZonedDateTimeCellFactory.create(eventTime.getAsString());
		} else {
			m_time = DataType.getMissingCell();
		}
		m_description = setStringCellValue(description);
	}

	protected DataCell setStringCellValue(final JsonElement aElem) {
		if (aElem != null) {
			return new StringCell(aElem.getAsString());
		} else {
			return DataType.getMissingCell();
		}
	}
}

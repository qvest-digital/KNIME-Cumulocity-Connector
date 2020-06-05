package de.tarent.cumulocity.data;

import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import de.tarent.cumulocity.data.events.IoTEvent;

/**
 * 
 * @author tarent solutions GmbH
 *
 */
public class MyJsonSerializer {

	public static Gson getInstance() {
		final Gson gson = new GsonBuilder().registerTypeAdapter(IoTEvent.class, new EventDeserializer()).create();

		return gson;
	}

	public static Gson getInstance(final int aMinContextLength) {
		return getInstance();
	}

	private static final class EventDeserializer implements JsonDeserializer<IoTEvent> {

		@Override
		public IoTEvent deserialize(final JsonElement json, final Type typeOfT,
				final JsonDeserializationContext context) throws JsonParseException {
			final JsonObject elem = json.getAsJsonObject();
			final JsonObject source = elem.get("source").getAsJsonObject();
			return new IoTEvent(elem.get("id"), elem.get("type"), elem.get("creationTime"), source.get("name"), 
					source.get("id"), elem.get("time"), elem.get("text"));
		}
	}

}
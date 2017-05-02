/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.common.schedule;

import co.cask.cdap.internal.schedule.trigger.Trigger;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import javax.annotation.Nullable;

/**
 * Serialization and deserialization of Triggers as Json.
 *
 * We serialize the classname of the Trigger object, and we use that during deserialization to determine the actual
 * subclass.
 */
public class TriggerCodec implements JsonSerializer<Trigger>, JsonDeserializer<Trigger> {

  // we use a separate GSON object to avoid recursion
  private static final Gson GSON = new Gson();

  @Nullable
  private JsonElement serializeTrigger(@Nullable Trigger trigger,
                                       JsonSerializationContext jsonSerializationContext) {
    if (trigger == null) {
      return null;
    }
    JsonArray jsonArray = new JsonArray();
    jsonArray.add(jsonSerializationContext.serialize(trigger.getClass().getName()));
    jsonArray.add(GSON.toJsonTree(trigger));
    return jsonArray;
  }

  @Nullable
  private Trigger deserializeTrigger(@Nullable JsonElement triggerJson) {
    if (triggerJson == null) {
      return null;
    }
    JsonArray jsonArray = triggerJson.getAsJsonArray();
    // the classname is serialized as the first element, the value is serialized as the second
    Class<? extends Trigger> triggerClass = forName(jsonArray.get(0).getAsString());
    return GSON.fromJson(jsonArray.get(1), triggerClass);
  }

  @SuppressWarnings("unchecked")
  private Class<? extends Trigger> forName(String className) {
    try {
      return (Class<? extends Trigger>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public JsonElement serialize(Trigger trigger, Type type, JsonSerializationContext jsonSerializationContext) {
    return serializeTrigger(trigger, jsonSerializationContext);
  }

  @Override
  public Trigger deserialize(JsonElement jsonElement, Type type,
                             JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
    return deserializeTrigger(jsonElement);
  }
}

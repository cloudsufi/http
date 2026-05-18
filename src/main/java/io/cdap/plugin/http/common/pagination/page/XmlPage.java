/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.plugin.http.common.pagination.page;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.http.common.http.HttpResponse;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XPathCompiler;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XdmValue;
import net.sf.saxon.trans.XPathException;

import java.util.Iterator;
import java.util.Map;

/**
 * Returns sub elements which are specified by XPath, one by one.
 * If primitive is specified by XPath only inner value is returned.
 * If non-primitive text representation of xml for that XPath is returned.
 */
class XmlPage extends BasePage {
  private final Map<String, String> fieldsMapping;
  private final Iterator<JsonElement> iterator;
  private final XdmNode document;
  private final Schema schema;
  private final BaseHttpSourceConfig config;

  private final Processor processor = new Processor(false);

  XmlPage(BaseHttpSourceConfig config, HttpResponse httpResponse) {
    super(httpResponse);
    this.config = config;
    this.fieldsMapping = config.getFullFieldsMapping();
    this.document = XmlUtil.createXmlDocument(processor, httpResponse.getBody());
    this.iterator = getDocumentElementsIterator();
    this.schema = config.getSchema();
  }

  @Override
  public boolean hasNext() {
    return this.iterator.hasNext();
  }

  @Override
  public PageEntry next() {
    String nodeString = this.iterator.next().getAsJsonObject().toString();
    try {
      StructuredRecord record = StructuredRecordStringConverter.fromJsonString(nodeString, schema);
      return new PageEntry(record);
    } catch (Throwable e) {
      return new PageEntry(InvalidEntryCreator.buildStringError(nodeString, e), config.getErrorHandling());
    }
  }

  /**
   * Get primitive element by XPath from document. If not found returns null.
   * If element is not a primitive (xml node with children) exception is thrown.
   *
   * @param path XPath
   * @return a primitive found by XPath
   */
  @Override
  public String getPrimitiveByPath(String path) {
    return XmlUtil.getByXPath(processor, document, path);
  }

  /**
   * 1. Converts xml to a structure which is defined by "Fields Mapping" configuration. This is done using saxon.
   * 2. The result entity is a json array.
   * 3. An iterator for elements of json array is returned.
   *
   * @return an iterator for elements of result json array.
   */
  private Iterator<JsonElement> getDocumentElementsIterator() {
    XPathCompiler xPathCompiler = processor.newXPathCompiler();
    JsonArray jsonArray = new JsonArray();
    try {
      for (XdmItem entry : xPathCompiler.evaluate(config.getResultPath(), document)) {
        JsonObject jsonObject = new JsonObject();
        for (String schemaFieldName : fieldsMapping.keySet()) {
          XdmValue xdmItems = xPathCompiler.evaluate(fieldsMapping.get(schemaFieldName), entry);
          String value = getValueFromXdmItem(xdmItems);
          jsonObject.addProperty(schemaFieldName, value);
        }
        jsonArray.add(jsonObject);
      }
    } catch (SaxonApiException | XPathException e) {
      throw new RuntimeException(e);
    }
    return jsonArray.iterator();
  }

  private String getValueFromXdmItem(XdmValue xdmItems) throws XPathException {
    StringBuilder value = new StringBuilder();
    int[] i = new int[1];
    ((XdmNode) xdmItems).children().iterator().forEachRemaining(t -> i[0] = i[0] + 1);
    // If main node contains child node, return full node else value of the node
    if (i[0] > 1) {
      value.append(xdmItems);
    } else {
      value.append(xdmItems.getUnderlyingValue().getStringValue());
    }
    return value.toString();
  }

  @Override
  public void close() {

  }
}

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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.nerdforge.unxml.parsers.Parser;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XPathCompiler;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.trans.XPathException;
import org.w3c.dom.Node;

import java.io.StringReader;
import java.io.StringWriter;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.XPathFactory;

/**
 * Utility functions for working with xml document.
 */
public class XmlUtil {
  private static XPathFactory xPathfactory = XPathFactory.newInstance();

  /**
   * Create xml document instance out of a String.
   *
   * @param processor Saxon processor with xml document configuration
   * @param xmlString xml in string format
   * @return a XdmNode Document instance representing input xml
   */
  public static XdmNode createXmlDocument(Processor processor, String xmlString) {
    DocumentBuilder documentBuilder = processor.newDocumentBuilder();
    XdmNode document = null;
    try {
      document = documentBuilder.build(new StreamSource(new StringReader(xmlString)));
    } catch (SaxonApiException e) {
      throw new RuntimeException(e);
    }
    return document;
  }

  /**
   * Get a parser used by unxml to parse nodes.
   * This parser returns value of node. If only one single node is present.
   * If multiple nodes or node tree is found, it's xml text representation is returned
   *
   * @return unxml parser
   */
  public static Parser<JsonNode> xmlTextNodeParser() {
    return node -> {
      if (node.getChildNodes().getLength() == 1) {
        return new TextNode(node.getTextContent());
      } else {
        return new TextNode(nodeToString(node));
      }
    };
  }

  /**
   * Converts xml node object. Into it's text representation
   *
   * @param node a node object
   * @return xml text representation of object
   */
  public static String nodeToString(Node node) {
    StringWriter sw = new StringWriter();
    try {
      Transformer t = TransformerFactory.newInstance().newTransformer();
      t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
      t.setOutputProperty(OutputKeys.INDENT, "yes");
      t.transform(new DOMSource(node), new StreamResult(sw));
    } catch (TransformerException e) {
      throw new IllegalStateException("Failed to parse xml document", e);
    }
    return sw.toString();
  }

  /**
   * Get element of given type from given document by XPath.
   * Throws an exception if element is not of given path.
   * Returns null if element not found
   *
   * @param processor Saxon processor with xml document configuration
   * @param document XdmNode document instance
   * @param path xpath string representation
   * @return element found by XPath or null if not found.
   */
  public static String getByXPath(Processor processor, XdmNode document, String path) {
    XPathCompiler xPathCompiler = processor.newXPathCompiler();
    try {
      return xPathCompiler.evaluate(path, document).getUnderlyingValue()
        .getStringValue();
    } catch (XPathException | SaxonApiException e) {
      return null;
    }
  }
}

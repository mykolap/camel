/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.xslt;

import java.util.Map;

import javax.xml.transform.TransformerFactory;

import org.apache.camel.Endpoint;
import org.apache.camel.builder.xml.XsltBuilder;
import org.apache.camel.component.ResourceBasedComponent;
import org.apache.camel.converter.jaxp.XmlConverter;
import org.apache.camel.impl.ProcessorEndpoint;
import org.apache.camel.util.CamelContextHelper;
import org.springframework.core.io.Resource;

/**
 * An <a href="http://camel.apache.org/xslt.html">XSLT Component</a>
 * for performing XSLT transforms of messages
 *
 * @version $Revision$
 */
public class XsltComponent extends ResourceBasedComponent {
    private XmlConverter xmlConverter;

    public XmlConverter getXmlConverter() {
        return xmlConverter;
    }

    public void setXmlConverter(XmlConverter xmlConverter) {
        this.xmlConverter = xmlConverter;
    }

    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        Resource resource = resolveMandatoryResource(remaining);
        if (log.isDebugEnabled()) {
            log.debug(this + " using schema resource: " + resource);
        }
        XsltBuilder xslt = getCamelContext().getInjector().newInstance(XsltBuilder.class);

        // lets allow the converter to be configured
        XmlConverter converter = null;
        String converterName = getAndRemoveParameter(parameters, "converter", String.class);        
        if (converterName != null) {
            converter = CamelContextHelper.mandatoryLookup(getCamelContext(), converterName, XmlConverter.class);
        }
        if (converter == null) {
            converter = getXmlConverter();
        }
        if (converter != null) {
            xslt.setConverter(converter);
        }
        
        String transformerFactoryClassName = getAndRemoveParameter(parameters, "transformerFactoryClass", String.class);
        TransformerFactory factory = null;
        if (transformerFactoryClassName != null) {
            // provide the class loader of this component to work in OSGi environments
            Class<?> factoryClass = getCamelContext().getClassResolver().resolveClass(transformerFactoryClassName, XsltComponent.class.getClassLoader());
            if (factoryClass != null) {
                factory = (TransformerFactory) getCamelContext().getInjector().newInstance(factoryClass);
            } else {
                log.warn("Cannot find the TransformerFactoryClass with the class name: " + transformerFactoryClassName);
            }
        }
        
        String transformerFactoryName = getAndRemoveParameter(parameters, "transformerFactory", String.class);        
        if (transformerFactoryName != null) {
            factory = CamelContextHelper.mandatoryLookup(getCamelContext(), transformerFactoryName, TransformerFactory.class);
        }
        
        if (factory != null) {
            xslt.getConverter().setTransformerFactory(factory);
        }
        xslt.setTransformerInputStream(resource.getInputStream());
        configureXslt(xslt, uri, remaining, parameters);
        return new ProcessorEndpoint(uri, this, xslt);
    }

    protected void configureXslt(XsltBuilder xslt, String uri, String remaining, Map<String, Object> parameters) throws Exception {
        setProperties(xslt, parameters);
    }
}

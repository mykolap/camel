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
package org.apache.camel.component.smpp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.camel.AsyncProcessor;
import org.apache.camel.Processor;
import org.apache.camel.Consumer;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.util.AsyncProcessorConverterHelper;
import org.jsmpp.DefaultPDUReader;
import org.jsmpp.DefaultPDUSender;
import org.jsmpp.SynchronizedPDUSender;
import org.jsmpp.bean.BindType;
import org.jsmpp.bean.NumberingPlanIndicator;
import org.jsmpp.bean.TypeOfNumber;
import org.jsmpp.extra.SessionState;
import org.jsmpp.session.*;
import org.jsmpp.util.DefaultComposer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link Consumer} which use the SMPP protocol
 * 
 * @version 
 */
public class SmppConsumer extends DefaultConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(SmppConsumer.class);

    private SmppConfiguration configuration;
    private Map<Integer, SessionStructure> sessions;

    private final ReentrantLock reconnectLock = new ReentrantLock();

    private final AsyncProcessor processor;
    private final SmppEndpoint endpoint;
    private final int concurrentConsumers;

    /**
     * The constructor which gets a smpp endpoint, a smpp configuration and a
     * processor
     */
    public SmppConsumer(SmppEndpoint endpoint, SmppConfiguration config, Processor processor) {
        super(endpoint, processor);

        this.processor = AsyncProcessorConverterHelper.convert(processor);
        this.endpoint = endpoint;
        this.concurrentConsumers = config.getConcurrentConsumers();
        this.sessions = new HashMap<>(this.concurrentConsumers);

        this.configuration = config;
    }

    @Override
    protected void doStart() throws Exception {
        LOG.debug("Connecting to: " + getEndpoint().getConnectionString() + "...");

        for (int i = 0; i < concurrentConsumers; i++) {
            SessionStructure sessionStructure = createSession(i);
            sessions.put(i, sessionStructure);
        }

        super.doStart();

        LOG.info("Connected to: " + getEndpoint().getConnectionString());
    }

    private SessionStructure createSession(int index) throws IOException {

        SessionStructure sessionStructure = new SessionStructure();

        sessionStructure.sessionStateListener = new SessionStateListener() {
            public void onStateChange(SessionState newState, SessionState oldState, Session source) {
                if (configuration.getSessionStateListener() != null) {
                    configuration.getSessionStateListener().onStateChange(newState, oldState, source);
                }

                if (newState.equals(SessionState.CLOSED)) {
                    LOG.warn("Lost connection to " + index + ": {} - trying to reconnect...", getEndpoint().getConnectionString());
                    closeSession(index);
                    reconnect(index, configuration.getInitialReconnectDelay());
                }
            }
        };
        MessageReceiverListener messageReceiverListener = new MessageReceiverListenerImpl(getEndpoint(), getProcessor(), getExceptionHandler());

        sessionStructure.smppSession = createSMPPSession();

        //By default in JSMPP value is 3, don't set less than default value
        if (this.configuration.getPduProcessorDegree() > 3) {
            sessionStructure.smppSession.setPduProcessorDegree(this.configuration.getPduProcessorDegree());
        }
        sessionStructure.smppSession.setEnquireLinkTimer(configuration.getEnquireLinkTimer());
        sessionStructure.smppSession.setTransactionTimer(configuration.getTransactionTimer());
        sessionStructure.smppSession.addSessionStateListener(sessionStructure.sessionStateListener);
        sessionStructure.smppSession.setMessageReceiverListener(messageReceiverListener);
        sessionStructure.smppSession.connectAndBind(this.configuration.getHost(), this.configuration.getPort(),
                new BindParameter(BindType.BIND_RX, this.configuration.getSystemId(),
                        this.configuration.getPassword(), this.configuration.getSystemType(),
                        TypeOfNumber.UNKNOWN, NumberingPlanIndicator.UNKNOWN,
                                  configuration.getAddressRange()));

        return sessionStructure;

    }
    
    /**
     * Factory method to easily instantiate a mock SMPPSession
     * 
     * @return the SMPPSession
     */
    SMPPSession createSMPPSession() {
        return new SMPPSession(new SynchronizedPDUSender(new DefaultPDUSender(
                    new DefaultComposer())), new DefaultPDUReader(), SmppConnectionFactory
                    .getInstance(configuration));
    }

    @Override
    protected void doStop() throws Exception {
        LOG.debug("Disconnecting from: " + getEndpoint().getConnectionString() + "...");

        for (int i = 0; i < sessions.size(); i++) {
            for (int j = 0; j < 3; j++) {
                try {
                    closeSession(i);
                } catch (Exception e) {
                    LOG.warn("Cannot close session " + i + " due to " + e.getMessage());
                }
            }
        }

        LOG.info("Disconnected from: " + getEndpoint().getConnectionString());
        super.doStop();
    }

    private void closeSession(int index) {
        SessionStructure sessionStructure = sessions.get(index);
        if (sessionStructure.smppSession != null) {
            sessionStructure.smppSession.removeSessionStateListener(sessionStructure.sessionStateListener);
            // remove this hack after http://code.google.com/p/jsmpp/issues/detail?id=93 is fixed
            try {
                Thread.sleep(1000);
                sessionStructure.smppSession.unbindAndClose();
            } catch (Exception e) {
                LOG.warn("Cannot close session " + index + " due to " + e.getMessage());
            }
            sessionStructure.smppSession = null;
            sessionStructure.sessionStateListener = null;
        }
    }

    private void reconnect(final int index, final long initialReconnectDelay) {
        if (reconnectLock.tryLock()) {
            try {
                Runnable r = new Runnable() {
                    public void run() {
                        boolean reconnected = false;
                        
                        LOG.info("Schedule reconnect after " + initialReconnectDelay + " millis");
                        try {
                            Thread.sleep(initialReconnectDelay);
                        } catch (InterruptedException e) {
                            // ignore
                        }

                        int attempt = 0;
                        SessionStructure sessionStructure = sessions.get(index);
                        while (!(isStopping() || isStopped())
                                && (sessionStructure.smppSession == null || sessionStructure.smppSession.getSessionState().equals(SessionState.CLOSED))) {
                            try {
                                LOG.info("Trying to reconnect to " + getEndpoint().getConnectionString() + " - attempt #" + (++attempt) + "...");
                                sessionStructure = createSession(index);
                                sessions.put(index, sessionStructure);
                                reconnected = true;
                            } catch (IOException e) {
                                LOG.info("Failed to reconnect to " + getEndpoint().getConnectionString());
                                closeSession(index);
                                try {
                                    Thread.sleep(configuration.getReconnectDelay());
                                } catch (InterruptedException ee) {
                                }
                            }
                        }
                        
                        if (reconnected) {
                            LOG.info("Reconnected to " + getEndpoint().getConnectionString());                        
                        }
                    }
                };
                
                Thread t = new Thread(r);
                t.start(); 
                t.join();
            } catch (InterruptedException e) {
                // noop
            }  finally {
                reconnectLock.unlock();
            }
        }
    }

    @Override
    public String toString() {
        return "SmppConsumer[" + getEndpoint().getConnectionString() + "]";
    }

    @Override
    public SmppEndpoint getEndpoint() {
        return (SmppEndpoint) super.getEndpoint();
    }

    /**
     * Returns the smpp configuration
     * 
     * @return the configuration
     */
    public SmppConfiguration getConfiguration() {
        return configuration;
    }

    private class SessionStructure {
        private SMPPSession smppSession;
        private SessionStateListener sessionStateListener;
    }

}

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

import java.nio.charset.Charset;

import org.apache.camel.Message;
import org.jsmpp.bean.Alphabet;
import org.jsmpp.extra.NegativeResponseException;
import org.jsmpp.session.SMPPSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SmppSmCommand extends AbstractSmppCommand {

    // FIXME: these constants should be defined somewhere in jSMPP:
    public static final int SMPP_NEG_RESPONSE_MSG_TOO_LONG = 1;

    private final Logger logger = LoggerFactory.getLogger(SmppSmCommand.class);

    public SmppSmCommand(SMPPSession session, SmppConfiguration config) {
        super(session, config);
    }

    protected byte[][] splitBody(Message message) throws SmppException {
        byte[] shortMessage = getShortMessage(message);
        SmppSplitter splitter = createSplitter(message);
        byte[][] segments = splitter.split(shortMessage);
        if (segments.length > 1) {
            // Message body is split into multiple parts,
            // check if this is permitted
            SmppSplittingPolicy policy = getSplittingPolicy(message);
            switch(policy) {
            case ALLOW:
                return segments;
            case TRUNCATE:
                return new byte[][] {java.util.Arrays.copyOfRange(shortMessage, 0, segments[0].length)};
            case REJECT:
                // FIXME - JSMPP needs to have an enum of the negative response
                // codes instead of just using them like this
                NegativeResponseException nre = new NegativeResponseException(SMPP_NEG_RESPONSE_MSG_TOO_LONG);
                throw new SmppException(nre);
            default:
                throw new SmppException("Unknown splitting policy: " + policy);
            }
        } else {
            return segments;
        }
    }

    private SmppSplittingPolicy getSplittingPolicy(Message message) throws SmppException {
        if (message.getHeaders().containsKey(SmppConstants.SPLITTING_POLICY)) {
            String policyName = message.getHeader(SmppConstants.SPLITTING_POLICY, String.class);
            return SmppSplittingPolicy.fromString(policyName);
        }
        return config.getSplittingPolicy();
    }

    protected SmppSplitter createSplitter(Message message) {

        Object objectBody = message.getBody();
        byte[] body;
        if (objectBody == null) {
            body = new byte[0];
        } else if (objectBody instanceof String) {
            body = ((String) objectBody).getBytes();
        } else {
            body = (byte[]) message.getBody();
        }

        Alphabet alphabet = determineAlphabet(message, body);

        SmppSplitter splitter;
        switch (alphabet) {
        case ALPHA_8_BIT:
            splitter = new Smpp8BitSplitter(body.length);
            break;
        case ALPHA_UCS2:
            splitter = new SmppUcs2Splitter(body.length/2);
            break;
        case ALPHA_DEFAULT:
        default:
            splitter = new SmppDefaultSplitter(body.length);
            break;
        }

        return splitter;
    }

    protected final byte[] getShortMessage(Message message) {
        return message.getBody(byte[].class);
    }

    private Alphabet determineAlphabet(Message message, byte[] body) {
        if (message.getHeaders().containsKey(SmppConstants.DATA_CODING)) {
            return Alphabet.parseDataCoding(message.getHeader(SmppConstants.DATA_CODING, Byte.class));
        } else {
            byte alphabet = config.getAlphabet();
            if (message.getHeaders().containsKey(SmppConstants.ALPHABET)) {
                alphabet = message.getHeader(SmppConstants.ALPHABET, Byte.class);
            }
            Alphabet alphabetObj;
            if (alphabet == SmppConstants.UNKNOWN_ALPHABET) {
                if (GSM0338Charset.isGsmChars(new String(body))) {
                    alphabetObj = Alphabet.ALPHA_DEFAULT;
                } else {
                    alphabetObj = Alphabet.ALPHA_UCS2;
                }
            } else {
                alphabetObj = Alphabet.valueOf(alphabet);
            }
            return alphabetObj;
        }
    }

}

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
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.DefaultExchange;
import org.jsmpp.bean.*;
import org.jsmpp.bean.OptionalParameter.OctetString;
import org.jsmpp.bean.OptionalParameter.Tag;
import org.jsmpp.session.SMPPSession;
import org.jsmpp.util.DeliveryReceiptState;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * JUnit test class for <code>org.apache.camel.component.smpp.SmppBinding</code>
 * 
 * @version 
 */
public class SmppBindingTest {
    
    private SmppBinding binding;

    @Before
    public void setUp() {
        binding = new SmppBinding() {
            Date getCurrentDate() {
                return new Date(1251666387000L);
            }
        };
    }

    @Test
    public void emptyConstructorShouldSetTheSmppConfiguration() {
        assertNotNull(binding.getConfiguration());
    }

    @Test
    public void constructorSmppConfigurationShouldSetTheSmppConfiguration() {
        SmppConfiguration configuration = new SmppConfiguration();
        binding = new SmppBinding(configuration);
        
        assertSame(configuration, binding.getConfiguration());
    }

    @Test
    public void createSmppMessageFromAlertNotificationShouldReturnASmppMessage() {
        AlertNotification alertNotification = new AlertNotification();
        alertNotification.setCommandId(1);
        alertNotification.setSequenceNumber(1);
        alertNotification.setSourceAddr("1616");
        alertNotification.setSourceAddrNpi(NumberingPlanIndicator.NATIONAL.value());
        alertNotification.setSourceAddrTon(TypeOfNumber.NATIONAL.value());
        alertNotification.setEsmeAddr("1717");
        alertNotification.setEsmeAddrNpi(NumberingPlanIndicator.NATIONAL.value());
        alertNotification.setEsmeAddrTon(TypeOfNumber.NATIONAL.value());
        SmppMessage smppMessage = binding.createSmppMessage(alertNotification);
        
        assertNull(smppMessage.getBody());
        assertEquals(10, smppMessage.getHeaders().size());
        assertEquals(1, smppMessage.getHeader(SmppConstants.SEQUENCE_NUMBER));
        assertEquals(1, smppMessage.getHeader(SmppConstants.COMMAND_ID));
        assertEquals(0, smppMessage.getHeader(SmppConstants.COMMAND_STATUS));
        assertEquals("1616", smppMessage.getHeader(SmppConstants.SOURCE_ADDR));
        assertEquals((byte) 8, smppMessage.getHeader(SmppConstants.SOURCE_ADDR_NPI));
        assertEquals((byte) 2, smppMessage.getHeader(SmppConstants.SOURCE_ADDR_TON));
        assertEquals("1717", smppMessage.getHeader(SmppConstants.ESME_ADDR));
        assertEquals((byte) 8, smppMessage.getHeader(SmppConstants.ESME_ADDR_NPI));
        assertEquals((byte) 2, smppMessage.getHeader(SmppConstants.ESME_ADDR_TON));
        assertEquals(SmppMessageType.AlertNotification.toString(), smppMessage.getHeader(SmppConstants.MESSAGE_TYPE));
    }

    @Test
    public void createSmppMessageFromDeliveryReceiptShouldReturnASmppMessage() throws Exception {
        DeliverSm deliverSm = new DeliverSm();
        deliverSm.setSmscDeliveryReceipt();
        deliverSm.setShortMessage("id:2 sub:001 dlvrd:001 submit date:0908312310 done date:0908312311 stat:DELIVRD err:xxx Text:Hello SMPP world!".getBytes());
        SmppMessage smppMessage = binding.createSmppMessage(deliverSm);
        
        assertEquals("Hello SMPP world!", smppMessage.getBody());
        assertEquals(9, smppMessage.getHeaders().size());
        assertEquals("2", smppMessage.getHeader(SmppConstants.ID));
        assertEquals(1, smppMessage.getHeader(SmppConstants.DELIVERED));
        // To avoid the test failure when running in different TimeZone
        //assertEquals(new Date(1251753060000L), smppMessage.getHeader(SmppConstants.DONE_DATE));
        assertEquals("xxx", smppMessage.getHeader(SmppConstants.ERROR));
        //assertEquals(new Date(1251753000000L), smppMessage.getHeader(SmppConstants.SUBMIT_DATE));
        assertEquals(1, smppMessage.getHeader(SmppConstants.SUBMITTED));
        assertEquals(DeliveryReceiptState.DELIVRD.toString(), smppMessage.getHeader(SmppConstants.FINAL_STATUS));
        assertEquals(SmppMessageType.DeliveryReceipt.toString(), smppMessage.getHeader(SmppConstants.MESSAGE_TYPE));
        assertNull(smppMessage.getHeader(SmppConstants.OPTIONAL_PARAMETERS));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void createSmppMessageFromDeliveryReceiptWithOptionalParametersShouldReturnASmppMessage() throws Exception {
        DeliverSm deliverSm = new DeliverSm();
        deliverSm.setSmscDeliveryReceipt();
        deliverSm.setShortMessage("id:2 sub:001 dlvrd:001 submit date:0908312310 done date:0908312311 stat:DELIVRD err:xxx Text:Hello SMPP world!".getBytes());
        deliverSm.setOptionalParameters(
            new OptionalParameter.OctetString(Tag.SOURCE_SUBADDRESS, "OctetString"),
            new OptionalParameter.COctetString(Tag.ADDITIONAL_STATUS_INFO_TEXT.code(), "COctetString"),
            new OptionalParameter.Byte(Tag.DEST_ADDR_SUBUNIT, (byte) 0x01),
            new OptionalParameter.Short(Tag.DEST_TELEMATICS_ID, (short) 1),
            new OptionalParameter.Int(Tag.QOS_TIME_TO_LIVE, 1),
            new OptionalParameter.Null(Tag.ALERT_ON_MESSAGE_DELIVERY));
        SmppMessage smppMessage = binding.createSmppMessage(deliverSm);

        assertEquals("Hello SMPP world!", smppMessage.getBody());
        assertEquals(17, smppMessage.getHeaders().size());
        assertEquals("2", smppMessage.getHeader(SmppConstants.ID));
        assertEquals(1, smppMessage.getHeader(SmppConstants.DELIVERED));
        // To avoid the test failure when running in different TimeZone
        //assertEquals(new Date(1251753060000L), smppMessage.getHeader(SmppConstants.DONE_DATE));
        assertEquals("xxx", smppMessage.getHeader(SmppConstants.ERROR));
        //assertEquals(new Date(1251753000000L), smppMessage.getHeader(SmppConstants.SUBMIT_DATE));
        assertEquals(1, smppMessage.getHeader(SmppConstants.SUBMITTED));
        assertEquals(DeliveryReceiptState.DELIVRD.toString(), smppMessage.getHeader(SmppConstants.FINAL_STATUS));
        assertEquals(SmppMessageType.DeliveryReceipt.toString(), smppMessage.getHeader(SmppConstants.MESSAGE_TYPE));

        //Map<String, Object> optionalParameters = smppMessage.getHeader(SmppConstants.OPTIONAL_PARAMETERS, Map.class);
        //assertEquals(6, optionalParameters.size());
        assertEquals("OctetString", new OctetString(Tag.SOURCE_SUBADDRESS.code(), (byte[])smppMessage.getHeader("SOURCE_SUBADDRESS")).getValueAsString());
        assertEquals("COctetString", smppMessage.getHeader("ADDITIONAL_STATUS_INFO_TEXT"));
        assertEquals(Byte.valueOf((byte) 0x01), smppMessage.getHeader("DEST_ADDR_SUBUNIT"));
        assertEquals(Short.valueOf((short) 1), smppMessage.getHeader("DEST_TELEMATICS_ID"));
        assertEquals(Integer.valueOf(1), smppMessage.getHeader("QOS_TIME_TO_LIVE"));
        assertNull("0x00", smppMessage.getHeader("ALERT_ON_MESSAGE_DELIVERY"));

        Map<Short, Object> optionalParameter = smppMessage.getHeader(SmppConstants.OPTIONAL_PARAMETER, Map.class);
        assertEquals(6, optionalParameter.size());
        assertArrayEquals("OctetString".getBytes("UTF-8"), (byte[]) optionalParameter.get(Short.valueOf((short) 0x0202)));
        assertEquals("COctetString", optionalParameter.get(Short.valueOf((short) 0x001D)));
        assertEquals(Byte.valueOf((byte) 0x01), optionalParameter.get(Short.valueOf((short) 0x0005)));
        assertEquals(Short.valueOf((short) 1), optionalParameter.get(Short.valueOf((short) 0x0008)));
        assertEquals(Integer.valueOf(1), optionalParameter.get(Short.valueOf((short) 0x0017)));
        assertNull("0x00", optionalParameter.get(Short.valueOf((short) 0x130C)));
    }

    @Test
    public void createSmppMessageFromDeliverSmShouldReturnASmppMessage() throws Exception {
        DeliverSm deliverSm = new DeliverSm();
        deliverSm.setShortMessage(GSM0338Charset.toGSM("Hello SMPP world!"));
        deliverSm.setSequenceNumber(1);
        deliverSm.setCommandId(1);
        deliverSm.setSourceAddr("1818");
        deliverSm.setSourceAddrNpi(NumberingPlanIndicator.NATIONAL.value());
        deliverSm.setSourceAddrTon(TypeOfNumber.NATIONAL.value());
        deliverSm.setDestAddress("1919");
        deliverSm.setDestAddrNpi(NumberingPlanIndicator.WAP.value());
        deliverSm.setDestAddrTon(TypeOfNumber.NETWORK_SPECIFIC.value());
        deliverSm.setScheduleDeliveryTime("090831230627004+");
        deliverSm.setValidityPeriod("090901230627004+");
        deliverSm.setServiceType("WAP");
        SmppMessage smppMessage = binding.createSmppMessage(deliverSm);
        
        assertArrayEquals(GSM0338Charset.toGSM("Hello SMPP world!"), (byte[]) smppMessage.getBody());
        assertEquals(15, smppMessage.getHeaders().size());
        assertEquals("Hello SMPP world!", smppMessage.getHeader(SmppConstants.DECODED_TEXT));
        assertEquals(1, smppMessage.getHeader(SmppConstants.SEQUENCE_NUMBER));
        assertEquals(1, smppMessage.getHeader(SmppConstants.COMMAND_ID));
        assertEquals("1818", smppMessage.getHeader(SmppConstants.SOURCE_ADDR));
        assertEquals((byte) 8, smppMessage.getHeader(SmppConstants.SOURCE_ADDR_NPI));
        assertEquals((byte) 2, smppMessage.getHeader(SmppConstants.SOURCE_ADDR_TON));
        assertEquals("1919", smppMessage.getHeader(SmppConstants.DEST_ADDR));
        assertEquals((byte) 18, smppMessage.getHeader(SmppConstants.DEST_ADDR_NPI));
        assertEquals((byte) 3, smppMessage.getHeader(SmppConstants.DEST_ADDR_TON));
        assertEquals("090831230627004+", smppMessage.getHeader(SmppConstants.SCHEDULE_DELIVERY_TIME));
        assertEquals("090901230627004+", smppMessage.getHeader(SmppConstants.VALIDITY_PERIOD));
        assertEquals("WAP", smppMessage.getHeader(SmppConstants.SERVICE_TYPE));
        assertEquals(SmppMessageType.DeliverSm.toString(), smppMessage.getHeader(SmppConstants.MESSAGE_TYPE));
    }
    
    @Test
    public void createSmppMessageFromDeliverSmWithPayloadInOptionalParameterShouldReturnASmppMessage() throws Exception {
        DeliverSm deliverSm = new DeliverSm();
        deliverSm.setSequenceNumber(1);
        deliverSm.setCommandId(1);
        deliverSm.setSourceAddr("1818");
        deliverSm.setSourceAddrNpi(NumberingPlanIndicator.NATIONAL.value());
        deliverSm.setSourceAddrTon(TypeOfNumber.NATIONAL.value());
        deliverSm.setDestAddress("1919");
        deliverSm.setDestAddrNpi(NumberingPlanIndicator.INTERNET.value());
        deliverSm.setDestAddrTon(TypeOfNumber.NETWORK_SPECIFIC.value());
        deliverSm.setScheduleDeliveryTime("090831230627004+");
        deliverSm.setValidityPeriod("090901230627004+");
        deliverSm.setServiceType("WAP");
        deliverSm.setOptionalParameters(new OctetString(OptionalParameter.Tag.MESSAGE_PAYLOAD.code(), GSM0338Charset.toGSM("Hello SMPP world!")));
        SmppMessage smppMessage = binding.createSmppMessage(deliverSm);
        
        assertArrayEquals(GSM0338Charset.toGSM("Hello SMPP world!"), (byte[]) smppMessage.getBody());
        assertEquals("Hello SMPP world!", smppMessage.getHeader(SmppConstants.DECODED_TEXT));
        assertEquals(18, smppMessage.getHeaders().size());
        assertEquals(1, smppMessage.getHeader(SmppConstants.SEQUENCE_NUMBER));
        assertEquals(1, smppMessage.getHeader(SmppConstants.COMMAND_ID));
        assertEquals("1818", smppMessage.getHeader(SmppConstants.SOURCE_ADDR));
        assertEquals((byte) 8, smppMessage.getHeader(SmppConstants.SOURCE_ADDR_NPI));
        assertEquals((byte) 2, smppMessage.getHeader(SmppConstants.SOURCE_ADDR_TON));
        assertEquals("1919", smppMessage.getHeader(SmppConstants.DEST_ADDR));
        assertEquals((byte) 14, smppMessage.getHeader(SmppConstants.DEST_ADDR_NPI));
        assertEquals((byte) 3, smppMessage.getHeader(SmppConstants.DEST_ADDR_TON));
        assertEquals("090831230627004+", smppMessage.getHeader(SmppConstants.SCHEDULE_DELIVERY_TIME));
        assertEquals("090901230627004+", smppMessage.getHeader(SmppConstants.VALIDITY_PERIOD));
        assertEquals("WAP", smppMessage.getHeader(SmppConstants.SERVICE_TYPE));
        assertEquals(SmppMessageType.DeliverSm.toString(), smppMessage.getHeader(SmppConstants.MESSAGE_TYPE));
    }
    
    @Test
    public void createSmppMessageFromDataSmShouldReturnASmppMessage() throws Exception {
        DataSm dataSm = new DataSm();
        dataSm.setSequenceNumber(1);
        dataSm.setCommandId(1);
        dataSm.setCommandStatus(0);
        dataSm.setSourceAddr("1818");
        dataSm.setSourceAddrNpi(NumberingPlanIndicator.NATIONAL.value());
        dataSm.setSourceAddrTon(TypeOfNumber.NATIONAL.value());
        dataSm.setDestAddress("1919");
        dataSm.setDestAddrNpi(NumberingPlanIndicator.NATIONAL.value());
        dataSm.setDestAddrTon(TypeOfNumber.NATIONAL.value());
        dataSm.setServiceType("WAP");
        dataSm.setRegisteredDelivery((byte) 0);
        SmppMessage smppMessage = binding.createSmppMessage(dataSm, "1");
        
        assertNull(smppMessage.getBody());
        assertEquals(14, smppMessage.getHeaders().size());
        assertEquals("1", smppMessage.getHeader(SmppConstants.ID));
        assertEquals(1, smppMessage.getHeader(SmppConstants.SEQUENCE_NUMBER));
        assertEquals(1, smppMessage.getHeader(SmppConstants.COMMAND_ID));
        assertEquals(0, smppMessage.getHeader(SmppConstants.COMMAND_STATUS));
        assertEquals("1818", smppMessage.getHeader(SmppConstants.SOURCE_ADDR));
        assertEquals((byte) 8, smppMessage.getHeader(SmppConstants.SOURCE_ADDR_NPI));
        assertEquals((byte) 2, smppMessage.getHeader(SmppConstants.SOURCE_ADDR_TON));
        assertEquals("1919", smppMessage.getHeader(SmppConstants.DEST_ADDR));
        assertEquals((byte) 8, smppMessage.getHeader(SmppConstants.DEST_ADDR_NPI));
        assertEquals((byte) 2, smppMessage.getHeader(SmppConstants.DEST_ADDR_TON));
        assertEquals("WAP", smppMessage.getHeader(SmppConstants.SERVICE_TYPE));
        assertEquals((byte) 0, smppMessage.getHeader(SmppConstants.REGISTERED_DELIVERY));
        assertEquals((byte) 0, smppMessage.getHeader(SmppConstants.DATA_CODING));
        assertEquals(SmppMessageType.DataSm.toString(), smppMessage.getHeader(SmppConstants.MESSAGE_TYPE));
    }

    @Test
    public void createSmppMessageFrom8bitDataCodingDeliverSmShouldNotModifyBody() throws Exception {
        final Set<String> encodings = Charset.availableCharsets().keySet();

        final byte[] dataCodings = {
            (byte)0x02,
            (byte)0x04,
            (byte)0xF6,
            (byte)0xF4
        };

        byte[] body = {
            (byte)0xFF, 'A', 'B', (byte)0x00,
            (byte)0xFF, (byte)0x7F, 'C', (byte)0xFF
        };

        DeliverSm deliverSm = new DeliverSm();

        for (byte dataCoding : dataCodings) {
            deliverSm.setDataCoding(dataCoding);
            deliverSm.setShortMessage(body);

            for (String encoding : encodings) {
                binding.getConfiguration().setEncoding(encoding);
                SmppMessage smppMessage = binding.createSmppMessage(deliverSm);
                assertArrayEquals(
                    String.format("data coding=0x%02X; encoding=%s",
                                  dataCoding,
                                  encoding),
                    body,
                    smppMessage.getBody(byte[].class));
            }
        }
    }

    @Test
    public void getterShouldReturnTheSetValues() {
        SmppConfiguration configuration = new SmppConfiguration();
        binding.setConfiguration(configuration);
        
        assertSame(configuration, binding.getConfiguration());
    }
    
    @Test
    public void createSmppSubmitSmCommand() {
        SMPPSession session = new SMPPSession();
        Exchange exchange = new DefaultExchange(new DefaultCamelContext());
        
        SmppCommand command = binding.createSmppCommand(session, exchange);
        
        assertTrue(command instanceof SmppSubmitSmCommand);
    }
    
    @Test
    public void createSmppSubmitMultiCommand() {
        SMPPSession session = new SMPPSession();
        Exchange exchange = new DefaultExchange(new DefaultCamelContext());
        exchange.getIn().setHeader(SmppConstants.COMMAND, "SubmitMulti");
        
        SmppCommand command = binding.createSmppCommand(session, exchange);
        
        assertTrue(command instanceof SmppSubmitMultiCommand);
    }
    
    @Test
    public void createSmppDataSmCommand() {
        SMPPSession session = new SMPPSession();
        Exchange exchange = new DefaultExchange(new DefaultCamelContext());
        exchange.getIn().setHeader(SmppConstants.COMMAND, "DataSm");
        
        SmppCommand command = binding.createSmppCommand(session, exchange);
        
        assertTrue(command instanceof SmppDataSmCommand);
    }
    
    @Test
    public void createSmppReplaceSmCommand() {
        SMPPSession session = new SMPPSession();
        Exchange exchange = new DefaultExchange(new DefaultCamelContext());
        exchange.getIn().setHeader(SmppConstants.COMMAND, "ReplaceSm");
        
        SmppCommand command = binding.createSmppCommand(session, exchange);
        
        assertTrue(command instanceof SmppReplaceSmCommand);
    }
    
    @Test
    public void createSmppQuerySmCommand() {
        SMPPSession session = new SMPPSession();
        Exchange exchange = new DefaultExchange(new DefaultCamelContext());
        exchange.getIn().setHeader(SmppConstants.COMMAND, "QuerySm");
        
        SmppCommand command = binding.createSmppCommand(session, exchange);
        
        assertTrue(command instanceof SmppQuerySmCommand);
    }
    
    @Test
    public void createSmppCancelSmCommand() {
        SMPPSession session = new SMPPSession();
        Exchange exchange = new DefaultExchange(new DefaultCamelContext());
        exchange.getIn().setHeader(SmppConstants.COMMAND, "CancelSm");
        
        SmppCommand command = binding.createSmppCommand(session, exchange);
        
        assertTrue(command instanceof SmppCancelSmCommand);
    }

    @Test
    public void createSmppMessageFrom4bitDataCodingDeliverSmUDHIBody() throws Exception {

        final byte dataCoding = (byte)0x04;

        byte[] body = {
                (byte)0x0B, (byte)0x05, (byte)0x04, (byte)0x23, (byte)0xF4, (byte)0x00, (byte)0x00, (byte)0x00,
                (byte)0x03, (byte)0x17, (byte)0x02, (byte)0x01, (byte)0x42, (byte)0x45, (byte)0x47, (byte)0x49,
                (byte)0x4E, (byte)0x3A, (byte)0x56, (byte)0x43, (byte)0x41, (byte)0x52, (byte)0x44, (byte)0x0D,
                (byte)0x0A, (byte)0x56, (byte)0x45, (byte)0x52, (byte)0x53, (byte)0x49, (byte)0x4F, (byte)0x4E,
                (byte)0x3A, (byte)0x32, (byte)0x2E, (byte)0x31, (byte)0x0D, (byte)0x0A, (byte)0x4E, (byte)0x3B,
                (byte)0x45, (byte)0x4E, (byte)0x43, (byte)0x4F, (byte)0x44, (byte)0x49, (byte)0x4E, (byte)0x47,
                (byte)0x3D, (byte)0x51, (byte)0x55, (byte)0x4F, (byte)0x54, (byte)0x45, (byte)0x44, (byte)0x2D,
                (byte)0x50, (byte)0x52, (byte)0x49, (byte)0x4E, (byte)0x54, (byte)0x41, (byte)0x42, (byte)0x4C,
                (byte)0x45, (byte)0x3B, (byte)0x43, (byte)0x48, (byte)0x41, (byte)0x52, (byte)0x53, (byte)0x45,
                (byte)0x54, (byte)0x3D, (byte)0x55, (byte)0x54, (byte)0x46, (byte)0x2D, (byte)0x38, (byte)0x3A,
                (byte)0x3B, (byte)0x3D, (byte)0x0D, (byte)0x0A, (byte)0x3D, (byte)0x44, (byte)0x30, (byte)0x3D,
                (byte)0x39, (byte)0x37, (byte)0x3D, (byte)0x44, (byte)0x30, (byte)0x3D, (byte)0x42, (byte)0x30,
                (byte)0x3D, (byte)0x44, (byte)0x30, (byte)0x3D, (byte)0x42, (byte)0x39, (byte)0x3D, (byte)0x44,
                (byte)0x31, (byte)0x3D, (byte)0x38, (byte)0x37, (byte)0x3D, (byte)0x44, (byte)0x30, (byte)0x3D,
                (byte)0x42, (byte)0x38, (byte)0x3D, (byte)0x44, (byte)0x30, (byte)0x3D, (byte)0x42, (byte)0x41,
                (byte)0x3B, (byte)0x3B, (byte)0x3B, (byte)0x0D, (byte)0x0A, (byte)0x54, (byte)0x45, (byte)0x4C,
                (byte)0x3B, (byte)0x56, (byte)0x4F, (byte)0x49, (byte)0x43, (byte)0x45, (byte)0x3B, (byte)0x43,
                (byte)0x45, (byte)0x4C, (byte)0x4C, (byte)0x3A
        };

        DeliverSm deliverSm = new DeliverSm();

        deliverSm.setDataCoding(dataCoding);
        deliverSm.setEsmClass(GSMSpecificFeature.UDHI.value());
        deliverSm.setShortMessage(body);

        SmppMessage smppMessage = binding.createSmppMessage(deliverSm);
        assertTrue(GSMSpecificFeature.UDHI.containedIn(deliverSm.getEsmClass()));
        //body includes UDHIE headers
        assertEquals(140, body.length);
        //decoded text doesn't include UDHIE headers
        String decodedText = smppMessage.getHeader(SmppConstants.DECODED_TEXT, String.class);
        assertEquals(128, decodedText.length());


    }

    @Test
    public void createSmppMessageFrom4bitDataCodingDeliverSmNonUDHIBody() throws Exception {

        final byte dataCoding = (byte)0x04;

        byte[] body = {
                (byte)0x0B, (byte)0x05, (byte)0x04, (byte)0x23, (byte)0xF4, (byte)0x00, (byte)0x00, (byte)0x00,
                (byte)0x03, (byte)0x17, (byte)0x02, (byte)0x01, (byte)0x42, (byte)0x45, (byte)0x47, (byte)0x49,
                (byte)0x4E, (byte)0x3A, (byte)0x56, (byte)0x43, (byte)0x41, (byte)0x52, (byte)0x44, (byte)0x0D,
                (byte)0x0A, (byte)0x56, (byte)0x45, (byte)0x52, (byte)0x53, (byte)0x49, (byte)0x4F, (byte)0x4E,
                (byte)0x3A, (byte)0x32, (byte)0x2E, (byte)0x31, (byte)0x0D, (byte)0x0A, (byte)0x4E, (byte)0x3B,
                (byte)0x45, (byte)0x4E, (byte)0x43, (byte)0x4F, (byte)0x44, (byte)0x49, (byte)0x4E, (byte)0x47,
                (byte)0x3D, (byte)0x51, (byte)0x55, (byte)0x4F, (byte)0x54, (byte)0x45, (byte)0x44, (byte)0x2D,
                (byte)0x50, (byte)0x52, (byte)0x49, (byte)0x4E, (byte)0x54, (byte)0x41, (byte)0x42, (byte)0x4C,
                (byte)0x45, (byte)0x3B, (byte)0x43, (byte)0x48, (byte)0x41, (byte)0x52, (byte)0x53, (byte)0x45,
                (byte)0x54, (byte)0x3D, (byte)0x55, (byte)0x54, (byte)0x46, (byte)0x2D, (byte)0x38, (byte)0x3A,
                (byte)0x3B, (byte)0x3D, (byte)0x0D, (byte)0x0A, (byte)0x3D, (byte)0x44, (byte)0x30, (byte)0x3D,
                (byte)0x39, (byte)0x37, (byte)0x3D, (byte)0x44, (byte)0x30, (byte)0x3D, (byte)0x42, (byte)0x30,
                (byte)0x3D, (byte)0x44, (byte)0x30, (byte)0x3D, (byte)0x42, (byte)0x39, (byte)0x3D, (byte)0x44,
                (byte)0x31, (byte)0x3D, (byte)0x38, (byte)0x37, (byte)0x3D, (byte)0x44, (byte)0x30, (byte)0x3D,
                (byte)0x42, (byte)0x38, (byte)0x3D, (byte)0x44, (byte)0x30, (byte)0x3D, (byte)0x42, (byte)0x41,
                (byte)0x3B, (byte)0x3B, (byte)0x3B, (byte)0x0D, (byte)0x0A, (byte)0x54, (byte)0x45, (byte)0x4C,
                (byte)0x3B, (byte)0x56, (byte)0x4F, (byte)0x49, (byte)0x43, (byte)0x45, (byte)0x3B, (byte)0x43,
                (byte)0x45, (byte)0x4C, (byte)0x4C, (byte)0x3A
        };

        DeliverSm deliverSm = new DeliverSm();

        deliverSm.setDataCoding(dataCoding);
        deliverSm.setEsmClass(GSMSpecificFeature.DEFAULT.value());
        deliverSm.setShortMessage(body);

        SmppMessage smppMessage = binding.createSmppMessage(deliverSm);
        assertFalse(GSMSpecificFeature.UDHI.containedIn(deliverSm.getEsmClass()));
        //body includes UDHIE headers
        assertEquals(140, body.length);
        //decoded text include UDHIE headers
        String decodedText = smppMessage.getHeader(SmppConstants.DECODED_TEXT, String.class);
        assertEquals(140, decodedText.length());

    }

    @Test
    public void createSmppMessageFrom16bitDataCodingDeliverSmUDHIEBody() throws Exception {

        final byte dataCoding = (byte)0x08;

        byte[] body = {
                (byte)0x05, (byte)0x00, (byte)0x03, (byte)0x82, (byte)0x03, (byte)0x01, (byte)0x00, (byte)0x53,
                (byte)0x00, (byte)0x20, (byte)0xD8, (byte)0x3C, (byte)0xDF, (byte)0x82, (byte)0x00, (byte)0x20,
                (byte)0xD8, (byte)0x3C, (byte)0xDF, (byte)0x81, (byte)0x00, (byte)0x20, (byte)0x00, (byte)0x4E
        };

        DeliverSm deliverSm = new DeliverSm();

        deliverSm.setDataCoding(dataCoding);
        deliverSm.setEsmClass(GSMSpecificFeature.UDHI.value());
        deliverSm.setShortMessage(body);

        SmppMessage smppMessage = binding.createSmppMessage(deliverSm);
        assertTrue(GSMSpecificFeature.UDHI.containedIn(deliverSm.getEsmClass()));
        //body includes UDHIE headers
        assertEquals(24, body.length);
        //decoded text include UDHIE headers
        String decodedText = smppMessage.getHeader(SmppConstants.DECODED_TEXT, String.class);
        assertEquals(9, decodedText.length());
        //System.out.println(Arrays.toString(decodedText.getBytes()));

    }

}
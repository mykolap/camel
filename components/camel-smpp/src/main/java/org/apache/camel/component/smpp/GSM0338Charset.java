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

//http://www.unicode.org/Public/MAPPINGS/ETSI/GSM0338.TXT

import java.util.*;

/**
 * \brief Provides services to translate UNICODE into GSM 03.38 character set and vice versa
 */
public class GSM0338Charset {
    /**
     * \brief Escape byte for the Unicode
     */

    public final static long serialVersionUID = 1_000_018L;

    protected static final Map<Character, Byte> UNICODE_TO_BYTE_MAP = Collections.unmodifiableMap(new HashMap<Character, Byte>() {
        {
            //main
            put((char) 0x0040, (byte) 0x00);//	0	@	COMMERCIAL AT
            put((char) 0x00A3, (byte) 0x01);//	1	£	POUND SIGN
            put((char) 0x0024, (byte) 0x02);//	2	$	DOLLAR SIGN
            put((char) 0x00A5, (byte) 0x03);//	3	¥	YEN SIGN
            put((char) 0x00E8, (byte) 0x04);//	4	è	LATIN SMALL LETTER E WITH GRAVE
            put((char) 0x00E9, (byte) 0x05);//	5	é	LATIN SMALL LETTER E WITH ACUTE
            put((char) 0x00F9, (byte) 0x06);//	6	ù	LATIN SMALL LETTER U WITH GRAVE
            put((char) 0x00EC, (byte) 0x07);//	7	ì	LATIN SMALL LETTER I WITH GRAVE
            put((char) 0x00F2, (byte) 0x08);//	8	ò	LATIN SMALL LETTER O WITH GRAVE
            put((char) 0x00E7, (byte) 0x09);//	9	ç	LATIN SMALL LETTER C WITH CEDILLA
            put((char) 0x000A, (byte) 0x0A);//	10	\n	LINE FEED
            put((char) 0x00D8, (byte) 0x0B);//	11	Ø	LATIN CAPITAL LETTER O WITH STROKE
            put((char) 0x00F8, (byte) 0x0C);//	12	ø	LATIN SMALL LETTER O WITH STROKE
            put((char) 0x000D, (byte) 0x0D);//	13	\r	CARRIAGE RETURN
            put((char) 0x00C5, (byte) 0x0E);//	14	Å	LATIN CAPITAL LETTER A WITH RING ABOVE
            put((char) 0x00E5, (byte) 0x0F);//	15	å	LATIN SMALL LETTER A WITH RING ABOVE
            put((char) 0x0394, (byte) 0x10);//	16	Δ	GREEK CAPITAL LETTER DELTA
            put((char) 0x005F, (byte) 0x11);//	17	_	LOW LINE
            put((char) 0x03A6, (byte) 0x12);//	18	Φ	GREEK CAPITAL LETTER PHI
            put((char) 0x0393, (byte) 0x13);//	19	Γ	GREEK CAPITAL LETTER GAMMA
            put((char) 0x039B, (byte) 0x14);//	20	Λ	GREEK CAPITAL LETTER LAMDA
            put((char) 0x03A9, (byte) 0x15);//	21	Ω	GREEK CAPITAL LETTER OMEGA
            put((char) 0x03A0, (byte) 0x16);//	22	Π	GREEK CAPITAL LETTER PI
            put((char) 0x03A8, (byte) 0x17);//	23	Ψ	GREEK CAPITAL LETTER PSI
            put((char) 0x03A3, (byte) 0x18);//	24	Σ	GREEK CAPITAL LETTER SIGMA
            put((char) 0x0398, (byte) 0x19);//	25	Θ	GREEK CAPITAL LETTER THETA
            put((char) 0x039E, (byte) 0x1A);//	26	Ξ	GREEK CAPITAL LETTER XI
            put((char) 0x00A0, (byte) 0x1B);//	27	 	ESCAPE TO EXTENSION TABLE (or displayed as NBSP, see note above)
            put((char) 0x00C6, (byte) 0x1C);//	28	Æ	LATIN CAPITAL LETTER AE
            put((char) 0x00E6, (byte) 0x1D);//	29	æ	LATIN SMALL LETTER AE
            put((char) 0x00DF, (byte) 0x1E);//	30	ß	LATIN SMALL LETTER SHARP S (German)
            put((char) 0x00C9, (byte) 0x1F);//	31	É	LATIN CAPITAL LETTER E WITH ACUTE
            put((char) 0x0020, (byte) 0x20);//	32	 	SPACE
            put((char) 0x0021, (byte) 0x21);//	33	!	EXCLAMATION MARK
            put((char) 0x0022, (byte) 0x22);//	34	"	QUOTATION MARK
            put((char) 0x0023, (byte) 0x23);//	35	#	NUMBER SIGN
            put((char) 0x00A4, (byte) 0x24);//	36	¤	CURRENCY SIGN
            put((char) 0x0025, (byte) 0x25);//	37	%	PERCENT SIGN
            put((char) 0x0026, (byte) 0x26);//	38	&	AMPERSAND
            put((char) 0x0027, (byte) 0x27);//	39	'	APOSTROPHE
            put((char) 0x0028, (byte) 0x28);//	40	(	LEFT PARENTHESIS
            put((char) 0x0029, (byte) 0x29);//	41	)	RIGHT PARENTHESIS
            put((char) 0x002A, (byte) 0x2A);//	42	*	ASTERISK
            put((char) 0x002B, (byte) 0x2B);//	43	+	PLUS SIGN
            put((char) 0x002C, (byte) 0x2C);//	44	,	COMMA
            put((char) 0x002D, (byte) 0x2D);//	45	-	HYPHEN-MINUS
            put((char) 0x002E, (byte) 0x2E);//	46	.	FULL STOP
            put((char) 0x002F, (byte) 0x2F);//	47	/	SOLIDUS
            put((char) 0x0030, (byte) 0x30);//	48	0	DIGIT ZERO
            put((char) 0x0031, (byte) 0x31);//	49	1	DIGIT ONE
            put((char) 0x0032, (byte) 0x32);//	50	2	DIGIT TWO
            put((char) 0x0033, (byte) 0x33);//	51	3	DIGIT THREE
            put((char) 0x0034, (byte) 0x34);//	52	4	DIGIT FOUR
            put((char) 0x0035, (byte) 0x35);//	53	5	DIGIT FIVE
            put((char) 0x0036, (byte) 0x36);//	54	6	DIGIT SIX
            put((char) 0x0037, (byte) 0x37);//	55	7	DIGIT SEVEN
            put((char) 0x0038, (byte) 0x38);//	56	8	DIGIT EIGHT
            put((char) 0x0039, (byte) 0x39);//	57	9	DIGIT NINE
            put((char) 0x003A, (byte) 0x3A);//	58	:	COLON
            put((char) 0x003B, (byte) 0x3B);//	59	;	SEMICOLON
            put((char) 0x003C, (byte) 0x3C);//	60	<	LESS-THAN SIGN
            put((char) 0x003D, (byte) 0x3D);//	61	=	EQUALS SIGN
            put((char) 0x003E, (byte) 0x3E);//	62	>	GREATER-THAN SIGN
            put((char) 0x003F, (byte) 0x3F);//	63	?	QUESTION MARK
            put((char) 0x00A1, (byte) 0x40);//	64	¡	INVERTED EXCLAMATION MARK
            put((char) 0x0041, (byte) 0x41);//	65	A	LATIN CAPITAL LETTER A
            put((char) 0x0042, (byte) 0x42);//	66	B	LATIN CAPITAL LETTER B
            put((char) 0x0043, (byte) 0x43);//	67	C	LATIN CAPITAL LETTER C
            put((char) 0x0044, (byte) 0x44);//	68	D	LATIN CAPITAL LETTER D
            put((char) 0x0045, (byte) 0x45);//	69	E	LATIN CAPITAL LETTER E
            put((char) 0x0046, (byte) 0x46);//	70	F	LATIN CAPITAL LETTER F
            put((char) 0x0047, (byte) 0x47);//	71	G	LATIN CAPITAL LETTER G
            put((char) 0x0048, (byte) 0x48);//	72	H	LATIN CAPITAL LETTER H
            put((char) 0x0049, (byte) 0x49);//	73	I	LATIN CAPITAL LETTER I
            put((char) 0x004A, (byte) 0x4A);//	74	J	LATIN CAPITAL LETTER J
            put((char) 0x004B, (byte) 0x4B);//	75	K	LATIN CAPITAL LETTER K
            put((char) 0x004C, (byte) 0x4C);//	76	L	LATIN CAPITAL LETTER L
            put((char) 0x004D, (byte) 0x4D);//	77	M	LATIN CAPITAL LETTER M
            put((char) 0x004E, (byte) 0x4E);//	78	N	LATIN CAPITAL LETTER N
            put((char) 0x004F, (byte) 0x4F);//	79	O	LATIN CAPITAL LETTER O
            put((char) 0x0050, (byte) 0x50);//	80	P	LATIN CAPITAL LETTER P
            put((char) 0x0051, (byte) 0x51);//	81	Q	LATIN CAPITAL LETTER Q
            put((char) 0x0052, (byte) 0x52);//	82	R	LATIN CAPITAL LETTER R
            put((char) 0x0053, (byte) 0x53);//	83	S	LATIN CAPITAL LETTER S
            put((char) 0x0054, (byte) 0x54);//	84	T	LATIN CAPITAL LETTER T
            put((char) 0x0055, (byte) 0x55);//	85	U	LATIN CAPITAL LETTER U
            put((char) 0x0056, (byte) 0x56);//	86	V	LATIN CAPITAL LETTER V
            put((char) 0x0057, (byte) 0x57);//	87	W	LATIN CAPITAL LETTER W
            put((char) 0x0058, (byte) 0x58);//	88	X	LATIN CAPITAL LETTER X
            put((char) 0x0059, (byte) 0x59);//	89	Y	LATIN CAPITAL LETTER Y
            put((char) 0x005A, (byte) 0x5A);//	90	Z	LATIN CAPITAL LETTER Z
            put((char) 0x00C4, (byte) 0x5B);//	91	Ä	LATIN CAPITAL LETTER A WITH DIAERESIS
            put((char) 0x00D6, (byte) 0x5C);//	92	Ö	LATIN CAPITAL LETTER O WITH DIAERESIS
            put((char) 0x00D1, (byte) 0x5D);//	93	Ñ	LATIN CAPITAL LETTER N WITH TILDE
            put((char) 0x00DC, (byte) 0x5E);//	94	Ü	LATIN CAPITAL LETTER U WITH DIAERESIS
            put((char) 0x00A7, (byte) 0x5F);//	95	§	SECTION SIGN
            put((char) 0x00BF, (byte) 0x60);//	96	¿	INVERTED QUESTION MARK
            put((char) 0x0061, (byte) 0x61);//	97	a	LATIN SMALL LETTER A
            put((char) 0x0062, (byte) 0x62);//	98	b	LATIN SMALL LETTER B
            put((char) 0x0063, (byte) 0x63);//	99	c	LATIN SMALL LETTER C
            put((char) 0x0064, (byte) 0x64);//	100	d	LATIN SMALL LETTER D
            put((char) 0x0065, (byte) 0x65);//	101	e	LATIN SMALL LETTER E
            put((char) 0x0066, (byte) 0x66);//	102	f	LATIN SMALL LETTER F
            put((char) 0x0067, (byte) 0x67);//	103	g	LATIN SMALL LETTER G
            put((char) 0x0068, (byte) 0x68);//	104	h	LATIN SMALL LETTER H
            put((char) 0x0069, (byte) 0x69);//	105	i	LATIN SMALL LETTER I
            put((char) 0x006A, (byte) 0x6A);//	106	j	LATIN SMALL LETTER J
            put((char) 0x006B, (byte) 0x6B);//	107	k	LATIN SMALL LETTER K
            put((char) 0x006C, (byte) 0x6C);//	108	l	LATIN SMALL LETTER L
            put((char) 0x006D, (byte) 0x6D);//	109	m	LATIN SMALL LETTER M
            put((char) 0x006E, (byte) 0x6E);//	110	n	LATIN SMALL LETTER N
            put((char) 0x006F, (byte) 0x6F);//	111	o	LATIN SMALL LETTER O
            put((char) 0x0070, (byte) 0x70);//	112	p	LATIN SMALL LETTER P
            put((char) 0x0071, (byte) 0x71);//	113	q	LATIN SMALL LETTER Q
            put((char) 0x0072, (byte) 0x72);//	114	r	LATIN SMALL LETTER R
            put((char) 0x0073, (byte) 0x73);//	115	s	LATIN SMALL LETTER S
            put((char) 0x0074, (byte) 0x74);//	116	t	LATIN SMALL LETTER T
            put((char) 0x0075, (byte) 0x75);//	117	u	LATIN SMALL LETTER U
            put((char) 0x0076, (byte) 0x76);//	118	v	LATIN SMALL LETTER V
            put((char) 0x0077, (byte) 0x77);//	119	w	LATIN SMALL LETTER W
            put((char) 0x0078, (byte) 0x78);//	120	x	LATIN SMALL LETTER X
            put((char) 0x0079, (byte) 0x79);//	121	y	LATIN SMALL LETTER Y
            put((char) 0x007A, (byte) 0x7A);//	122	z	LATIN SMALL LETTER Z
            put((char) 0x00E4, (byte) 0x7B);//	123	ä	LATIN SMALL LETTER A WITH DIAERESIS
            put((char) 0x00F6, (byte) 0x7C);//	124	ö	LATIN SMALL LETTER O WITH DIAERESIS
            put((char) 0x00F1, (byte) 0x7D);//	125	ñ	LATIN SMALL LETTER N WITH TILDE
            put((char) 0x00FC, (byte) 0x7E);//	126	ü	LATIN SMALL LETTER U WITH DIAERESIS
            put((char) 0x00E0, (byte) 0x7F);//	127	à	LATIN SMALL LETTER A WITH GRAVE
            //alternatives
            put((char) 0x0000, (byte) 0x00);//	0	 	NULL (see note above)
            put((char) 0x00C7, (byte) 0x09);//	9	Ç	LATIN CAPITAL LETTER C WITH CEDILLA (see note above)
            put((char) 0x0391, (byte) 0x41);//	65	Α	GREEK CAPITAL LETTER ALPHA
            put((char) 0x0392, (byte) 0x42);//	66	Β	GREEK CAPITAL LETTER BETA
            put((char) 0x0395, (byte) 0x45);//	69	Ε	GREEK CAPITAL LETTER EPSILON
            put((char) 0x0397, (byte) 0x48);//	72	Η	GREEK CAPITAL LETTER ETA
            put((char) 0x0399, (byte) 0x49);//	73	Ι	GREEK CAPITAL LETTER IOTA
            put((char) 0x039A, (byte) 0x4B);//	75	Κ	GREEK CAPITAL LETTER KAPPA
            put((char) 0x039C, (byte) 0x4D);//	77	Μ	GREEK CAPITAL LETTER MU
            put((char) 0x039D, (byte) 0x4E);//	78	Ν	GREEK CAPITAL LETTER NU
            put((char) 0x039F, (byte) 0x4F);//	79	Ο	GREEK CAPITAL LETTER OMICRON
            put((char) 0x03A1, (byte) 0x50);//	80	Ρ	GREEK CAPITAL LETTER RHO
            put((char) 0x03A4, (byte) 0x54);//	84	Τ	GREEK CAPITAL LETTER TAU
            put((char) 0x03A7, (byte) 0x58);//	88	Χ	GREEK CAPITAL LETTER CHI
            put((char) 0x03A5, (byte) 0x59);//	89	Υ	GREEK CAPITAL LETTER UPSILON
            put((char) 0x0396, (byte) 0x5A);//	90	Ζ	GREEK CAPITAL LETTER ZETA
        }
    });

    protected static final Map<Byte, Character> BYTE_TO_UNICODE_MAP = Collections.unmodifiableMap(new HashMap<Byte, Character>() {
        {
            put((byte) 0x00, (char) 0x0040);//	0	@	COMMERCIAL AT
            put((byte) 0x01, (char) 0x00A3);//	1	£	POUND SIGN
            put((byte) 0x02, (char) 0x0024);//	2	$	DOLLAR SIGN
            put((byte) 0x03, (char) 0x00A5);//	3	¥	YEN SIGN
            put((byte) 0x04, (char) 0x00E8);//	4	è	LATIN SMALL LETTER E WITH GRAVE
            put((byte) 0x05, (char) 0x00E9);//	5	é	LATIN SMALL LETTER E WITH ACUTE
            put((byte) 0x06, (char) 0x00F9);//	6	ù	LATIN SMALL LETTER U WITH GRAVE
            put((byte) 0x07, (char) 0x00EC);//	7	ì	LATIN SMALL LETTER I WITH GRAVE
            put((byte) 0x08, (char) 0x00F2);//	8	ò	LATIN SMALL LETTER O WITH GRAVE
            put((byte) 0x09, (char) 0x00E7);//	9	ç	LATIN SMALL LETTER C WITH CEDILLA
            put((byte) 0x0A, (char) 0x000A);//	10	\n	LINE FEED
            put((byte) 0x0B, (char) 0x00D8);//	11	Ø	LATIN CAPITAL LETTER O WITH STROKE
            put((byte) 0x0C, (char) 0x00F8);//	12	ø	LATIN SMALL LETTER O WITH STROKE
            put((byte) 0x0D, (char) 0x000D);//	13	\r	CARRIAGE RETURN
            put((byte) 0x0E, (char) 0x00C5);//	14	Å	LATIN CAPITAL LETTER A WITH RING ABOVE
            put((byte) 0x0F, (char) 0x00E5);//	15	å	LATIN SMALL LETTER A WITH RING ABOVE
            put((byte) 0x10, (char) 0x0394);//	16	Δ	GREEK CAPITAL LETTER DELTA
            put((byte) 0x11, (char) 0x005F);//	17	_	LOW LINE
            put((byte) 0x12, (char) 0x03A6);//	18	Φ	GREEK CAPITAL LETTER PHI
            put((byte) 0x13, (char) 0x0393);//	19	Γ	GREEK CAPITAL LETTER GAMMA
            put((byte) 0x14, (char) 0x039B);//	20	Λ	GREEK CAPITAL LETTER LAMDA
            put((byte) 0x15, (char) 0x03A9);//	21	Ω	GREEK CAPITAL LETTER OMEGA
            put((byte) 0x16, (char) 0x03A0);//	22	Π	GREEK CAPITAL LETTER PI
            put((byte) 0x17, (char) 0x03A8);//	23	Ψ	GREEK CAPITAL LETTER PSI
            put((byte) 0x18, (char) 0x03A3);//	24	Σ	GREEK CAPITAL LETTER SIGMA
            put((byte) 0x19, (char) 0x0398);//	25	Θ	GREEK CAPITAL LETTER THETA
            put((byte) 0x1A, (char) 0x039E);//	26	Ξ	GREEK CAPITAL LETTER XI
            put((byte) 0x1B, (char) 0x00A0);//	27	 	ESCAPE TO EXTENSION TABLE (or displayed as NBSP, see note above)
            put((byte) 0x1C, (char) 0x00C6);//	28	Æ	LATIN CAPITAL LETTER AE
            put((byte) 0x1D, (char) 0x00E6);//	29	æ	LATIN SMALL LETTER AE
            put((byte) 0x1E, (char) 0x00DF);//	30	ß	LATIN SMALL LETTER SHARP S (German)
            put((byte) 0x1F, (char) 0x00C9);//	31	É	LATIN CAPITAL LETTER E WITH ACUTE
            put((byte) 0x20, (char) 0x0020);//	32	 	SPACE
            put((byte) 0x21, (char) 0x0021);//	33	!	EXCLAMATION MARK
            put((byte) 0x22, (char) 0x0022);//	34	"	QUOTATION MARK
            put((byte) 0x23, (char) 0x0023);//	35	#	NUMBER SIGN
            put((byte) 0x24, (char) 0x00A4);//	36	¤	CURRENCY SIGN
            put((byte) 0x25, (char) 0x0025);//	37	%	PERCENT SIGN
            put((byte) 0x26, (char) 0x0026);//	38	&	AMPERSAND
            put((byte) 0x27, (char) 0x0027);//	39	'	APOSTROPHE
            put((byte) 0x28, (char) 0x0028);//	40	(	LEFT PARENTHESIS
            put((byte) 0x29, (char) 0x0029);//	41	)	RIGHT PARENTHESIS
            put((byte) 0x2A, (char) 0x002A);//	42	*	ASTERISK
            put((byte) 0x2B, (char) 0x002B);//	43	+	PLUS SIGN
            put((byte) 0x2C, (char) 0x002C);//	44	,	COMMA
            put((byte) 0x2D, (char) 0x002D);//	45	-	HYPHEN-MINUS
            put((byte) 0x2E, (char) 0x002E);//	46	.	FULL STOP
            put((byte) 0x2F, (char) 0x002F);//	47	/	SOLIDUS
            put((byte) 0x30, (char) 0x0030);//	48	0	DIGIT ZERO
            put((byte) 0x31, (char) 0x0031);//	49	1	DIGIT ONE
            put((byte) 0x32, (char) 0x0032);//	50	2	DIGIT TWO
            put((byte) 0x33, (char) 0x0033);//	51	3	DIGIT THREE
            put((byte) 0x34, (char) 0x0034);//	52	4	DIGIT FOUR
            put((byte) 0x35, (char) 0x0035);//	53	5	DIGIT FIVE
            put((byte) 0x36, (char) 0x0036);//	54	6	DIGIT SIX
            put((byte) 0x37, (char) 0x0037);//	55	7	DIGIT SEVEN
            put((byte) 0x38, (char) 0x0038);//	56	8	DIGIT EIGHT
            put((byte) 0x39, (char) 0x0039);//	57	9	DIGIT NINE
            put((byte) 0x3A, (char) 0x003A);//	58	:	COLON
            put((byte) 0x3B, (char) 0x003B);//	59	;	SEMICOLON
            put((byte) 0x3C, (char) 0x003C);//	60	<	LESS-THAN SIGN
            put((byte) 0x3D, (char) 0x003D);//	61	=	EQUALS SIGN
            put((byte) 0x3E, (char) 0x003E);//	62	>	GREATER-THAN SIGN
            put((byte) 0x3F, (char) 0x003F);//	63	?	QUESTION MARK
            put((byte) 0x40, (char) 0x00A1);//	64	¡	INVERTED EXCLAMATION MARK
            put((byte) 0x41, (char) 0x0041);//	65	A	LATIN CAPITAL LETTER A
            put((byte) 0x42, (char) 0x0042);//	66	B	LATIN CAPITAL LETTER B
            put((byte) 0x43, (char) 0x0043);//	67	C	LATIN CAPITAL LETTER C
            put((byte) 0x44, (char) 0x0044);//	68	D	LATIN CAPITAL LETTER D
            put((byte) 0x45, (char) 0x0045);//	69	E	LATIN CAPITAL LETTER E
            put((byte) 0x46, (char) 0x0046);//	70	F	LATIN CAPITAL LETTER F
            put((byte) 0x47, (char) 0x0047);//	71	G	LATIN CAPITAL LETTER G
            put((byte) 0x48, (char) 0x0048);//	72	H	LATIN CAPITAL LETTER H
            put((byte) 0x49, (char) 0x0049);//	73	I	LATIN CAPITAL LETTER I
            put((byte) 0x4A, (char) 0x004A);//	74	J	LATIN CAPITAL LETTER J
            put((byte) 0x4B, (char) 0x004B);//	75	K	LATIN CAPITAL LETTER K
            put((byte) 0x4C, (char) 0x004C);//	76	L	LATIN CAPITAL LETTER L
            put((byte) 0x4D, (char) 0x004D);//	77	M	LATIN CAPITAL LETTER M
            put((byte) 0x4E, (char) 0x004E);//	78	N	LATIN CAPITAL LETTER N
            put((byte) 0x4F, (char) 0x004F);//	79	O	LATIN CAPITAL LETTER O
            put((byte) 0x50, (char) 0x0050);//	80	P	LATIN CAPITAL LETTER P
            put((byte) 0x51, (char) 0x0051);//	81	Q	LATIN CAPITAL LETTER Q
            put((byte) 0x52, (char) 0x0052);//	82	R	LATIN CAPITAL LETTER R
            put((byte) 0x53, (char) 0x0053);//	83	S	LATIN CAPITAL LETTER S
            put((byte) 0x54, (char) 0x0054);//	84	T	LATIN CAPITAL LETTER T
            put((byte) 0x55, (char) 0x0055);//	85	U	LATIN CAPITAL LETTER U
            put((byte) 0x56, (char) 0x0056);//	86	V	LATIN CAPITAL LETTER V
            put((byte) 0x57, (char) 0x0057);//	87	W	LATIN CAPITAL LETTER W
            put((byte) 0x58, (char) 0x0058);//	88	X	LATIN CAPITAL LETTER X
            put((byte) 0x59, (char) 0x0059);//	89	Y	LATIN CAPITAL LETTER Y
            put((byte) 0x5A, (char) 0x005A);//	90	Z	LATIN CAPITAL LETTER Z
            put((byte) 0x5B, (char) 0x00C4);//	91	Ä	LATIN CAPITAL LETTER A WITH DIAERESIS
            put((byte) 0x5C, (char) 0x00D6);//	92	Ö	LATIN CAPITAL LETTER O WITH DIAERESIS
            put((byte) 0x5D, (char) 0x00D1);//	93	Ñ	LATIN CAPITAL LETTER N WITH TILDE
            put((byte) 0x5E, (char) 0x00DC);//	94	Ü	LATIN CAPITAL LETTER U WITH DIAERESIS
            put((byte) 0x5F, (char) 0x00A7);//	95	§	SECTION SIGN
            put((byte) 0x60, (char) 0x00BF);//	96	¿	INVERTED QUESTION MARK
            put((byte) 0x61, (char) 0x0061);//	97	a	LATIN SMALL LETTER A
            put((byte) 0x62, (char) 0x0062);//	98	b	LATIN SMALL LETTER B
            put((byte) 0x63, (char) 0x0063);//	99	c	LATIN SMALL LETTER C
            put((byte) 0x64, (char) 0x0064);//	100	d	LATIN SMALL LETTER D
            put((byte) 0x65, (char) 0x0065);//	101	e	LATIN SMALL LETTER E
            put((byte) 0x66, (char) 0x0066);//	102	f	LATIN SMALL LETTER F
            put((byte) 0x67, (char) 0x0067);//	103	g	LATIN SMALL LETTER G
            put((byte) 0x68, (char) 0x0068);//	104	h	LATIN SMALL LETTER H
            put((byte) 0x69, (char) 0x0069);//	105	i	LATIN SMALL LETTER I
            put((byte) 0x6A, (char) 0x006A);//	106	j	LATIN SMALL LETTER J
            put((byte) 0x6B, (char) 0x006B);//	107	k	LATIN SMALL LETTER K
            put((byte) 0x6C, (char) 0x006C);//	108	l	LATIN SMALL LETTER L
            put((byte) 0x6D, (char) 0x006D);//	109	m	LATIN SMALL LETTER M
            put((byte) 0x6E, (char) 0x006E);//	110	n	LATIN SMALL LETTER N
            put((byte) 0x6F, (char) 0x006F);//	111	o	LATIN SMALL LETTER O
            put((byte) 0x70, (char) 0x0070);//	112	p	LATIN SMALL LETTER P
            put((byte) 0x71, (char) 0x0071);//	113	q	LATIN SMALL LETTER Q
            put((byte) 0x72, (char) 0x0072);//	114	r	LATIN SMALL LETTER R
            put((byte) 0x73, (char) 0x0073);//	115	s	LATIN SMALL LETTER S
            put((byte) 0x74, (char) 0x0074);//	116	t	LATIN SMALL LETTER T
            put((byte) 0x75, (char) 0x0075);//	117	u	LATIN SMALL LETTER U
            put((byte) 0x76, (char) 0x0076);//	118	v	LATIN SMALL LETTER V
            put((byte) 0x77, (char) 0x0077);//	119	w	LATIN SMALL LETTER W
            put((byte) 0x78, (char) 0x0078);//	120	x	LATIN SMALL LETTER X
            put((byte) 0x79, (char) 0x0079);//	121	y	LATIN SMALL LETTER Y
            put((byte) 0x7A, (char) 0x007A);//	122	z	LATIN SMALL LETTER Z
            put((byte) 0x7B, (char) 0x00E4);//	123	ä	LATIN SMALL LETTER A WITH DIAERESIS
            put((byte) 0x7C, (char) 0x00F6);//	124	ö	LATIN SMALL LETTER O WITH DIAERESIS
            put((byte) 0x7D, (char) 0x00F1);//	125	ñ	LATIN SMALL LETTER N WITH TILDE
            put((byte) 0x7E, (char) 0x00FC);//	126	ü	LATIN SMALL LETTER U WITH DIAERESIS
            put((byte) 0x7F, (char) 0x00E0);//	127	à	LATIN SMALL LETTER A WITH GRAVE
        }
    });

    protected static final Map<Character, Byte> UNICODE_TO_ESCAPED_BYTE = Collections.unmodifiableMap(new HashMap<Character, Byte>() {
        {
            put((char) 0x000C, (byte) 0x0A);//	0x1B0A	10		FORM FEED
            put((char) 0x005E, (byte) 0x14);//	0x1B14	20	^	CIRCUMFLEX ACCENT
            put((char) 0x007B, (byte) 0x28);//	0x1B28	40	{	LEFT CURLY BRACKET
            put((char) 0x007D, (byte) 0x29);//	0x1B29	41	}	RIGHT CURLY BRACKET
            put((char) 0x005C, (byte) 0x2F);//	0x1B2F	47	\	REVERSE SOLIDUS
            put((char) 0x005B, (byte) 0x3C);//	0x1B3C	60	[	LEFT SQUARE BRACKET
            put((char) 0x007E, (byte) 0x3D);//	0x1B3D	61	~	TILDE
            put((char) 0x005D, (byte) 0x3E);//	0x1B3E	62	]	RIGHT SQUARE BRACKET
            put((char) 0x007C, (byte) 0x40);//	0x1B40	64	|	VERTICAL LINE
            put((char) 0x20AC, (byte) 0x65);//	0x1B65	101	€	EURO SIGN
        }
    });

    protected static final Map<Byte, Character> ESCAPED_BYTE_TO_UNICODE_MAP = Collections.unmodifiableMap(new HashMap<Byte, Character>() {
        {
            put((byte) 0x0A, (char) 0x000C);//	0x1B0A	10		FORM FEED
            put((byte) 0x14, (char) 0x005E);//	0x1B14	20	^	CIRCUMFLEX ACCENT
            put((byte) 0x28, (char) 0x007B);//	0x1B28	40	{	LEFT CURLY BRACKET
            put((byte) 0x29, (char) 0x007D);//	0x1B29	41	}	RIGHT CURLY BRACKET
            put((byte) 0x2F, (char) 0x005C);//	0x1B2F	47	\	REVERSE SOLIDUS
            put((byte) 0x3C, (char) 0x005B);//	0x1B3C	60	[	LEFT SQUARE BRACKET
            put((byte) 0x3D, (char) 0x007E);//	0x1B3D	61	~	TILDE
            put((byte) 0x3E, (char) 0x005D);//	0x1B3E	62	]	RIGHT SQUARE BRACKET
            put((byte) 0x40, (char) 0x007C);//	0x1B40	64	|	VERTICAL LINE
            put((byte) 0x65, (char) 0x20AC);//	0x1B65	101	€	EURO SIGN
        }
    });

    /**
     * Converts an unicode string to the GSM default charset encoded byte array.
     *
     * @param text
     *            String to convert to GSM charset
     * @return GSM encoded string
     */
    public static byte[] toGSM(String text) {

        if (text == null) {

            return new byte[0];

        } else {

            int len = text.length();
            List<Byte> byteList = new ArrayList<>((int) (len * 1.2));
            for (int i = 0; i < len; i++) {
                char uch = text.charAt(i);
                Byte currentByte = UNICODE_TO_BYTE_MAP.get(uch);
                if (currentByte == null) {
                    currentByte = UNICODE_TO_ESCAPED_BYTE.get(uch);
                    if (currentByte == null) {
                        byteList.add((byte)0x3F); //question mark
                    } else {
                        byteList.add((byte)0x1B); //escape character
                        byteList.add(currentByte);
                    }
                } else {
                    byteList.add(currentByte);
                }
            }

            byte[] byteArray = new byte[byteList.size()];
            int i = 0;
            for (Byte n : byteList) {
                byteArray[i++] = n;
            }
            return byteArray;

        }

    }

    /**
     * Converts from GSM default charset encoded byte array into unicode String
     *
     * @param bytes
     *            GSM character encoded array
     * @return Unicode string representing b
     */
    public static String toUnicode(byte[] bytes) {

        if (bytes == null) {
            return "";
        } else {
            int len = bytes.length;
            StringBuilder stringBuilder = new StringBuilder();

            boolean isEscaped = false;
            int nullsCounter = 0;
            Character previousChar = null;

            for (int i = 0; i < len; ++i) {
                byte currentByte = bytes[i];

                if (isEscaped) {
                    Character currentEscapedChar = ESCAPED_BYTE_TO_UNICODE_MAP.get(currentByte);
                    if (currentEscapedChar == null) {
                        stringBuilder.append(previousChar);
                    } else {
                        stringBuilder.append(currentEscapedChar);
                        isEscaped = false;
                        continue;
                    }
                }

                Character currentChar = BYTE_TO_UNICODE_MAP.get(currentByte);

                isEscaped = false;
                if (currentByte == 0x00) { //0x00 can be '@' or 0x0000
                    nullsCounter++;
                } else {
                    if (nullsCounter > 0) {
                        //current byte not null - previous convert to @, not 0x00
                        for (int j = 0; j < nullsCounter; j++) {
                            stringBuilder.append('@');
                        }
                        nullsCounter = 0;
                    }
                    if (currentByte == 0x1B) { //escape character
                        isEscaped = true;
                        previousChar = currentChar;
                    } else {
                        stringBuilder.append(currentChar);
                    }
                }

            }

            // if unclose escape or 0x00 present
            if (isEscaped) {
                //previous char was escape, but no second char
                //process as is
                stringBuilder.append(previousChar);
            }
            if (nullsCounter > 0) {
                //last char was null, convert last 0x00's to 0x0000
                for (int j = 0; j < nullsCounter; j++) {
                    stringBuilder.append((char)0x0000);
                }
            }

            return stringBuilder.toString();
        }

    }

    public static boolean isGsmChars (String text) {

        int len = text.length();
        for (int i = 0; i < len; i++) {
            char uch = text.charAt(i);
            if (!UNICODE_TO_BYTE_MAP.containsKey(uch)) {
                if (!UNICODE_TO_ESCAPED_BYTE.containsKey(uch)) {
                    return false;
                }
            }
        }
        return true;

    }

}
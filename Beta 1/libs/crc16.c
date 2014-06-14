/*
 * Standard CRC-16 implementation
 */

static unsigned short crc_16_table[16] = {
	0x0000, 0xCC01, 0xD801, 0x1400, 0xF001, 0x3C00, 0x2800, 0xE401,
	0xA001, 0x6C00, 0x7800, 0xB401, 0x5000, 0x9C01, 0x8801, 0x4400 
};

unsigned short crc16(unsigned short start, char *p, unsigned int len)
{
	unsigned short crc = start;
	int r;

	while (len--) {
		/* compute checksum of lower four bits of *p */
		r = crc_16_table[crc & 0xF];
		crc = (crc >> 4) & 0x0FFF;
		crc = crc ^ r ^ crc_16_table[*p & 0xF];

		/* now compute checksum of upper four bits of *p */
		r = crc_16_table[crc & 0xF];
		crc = (crc >> 4) & 0x0FFF;
		crc = crc ^ r ^ crc_16_table[(*p >> 4) & 0xF];
		/* next... */
		p++;
	}
	return crc;
}

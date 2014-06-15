#!/usr/bin/perl


my $inputFile = shift;

open (my $fh, '<', $inputFile) or die "FILE NO OPEN BITCH $!";

print "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
print "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n";
print "<Document>\n";

print '<Style id="smallgreen">
    <IconStyle>
		<color>ff00ff00</color>
		<scale>0.4</scale>
        <Icon>
            <href>http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png</href>
        </Icon>
    </IconStyle>
	 <LabelStyle>
    <scale>0</scale>
  </LabelStyle>
	</Style>
	<Style id="smallred">
    <IconStyle>
		<color>ff0000ff</color>
		<scale>0.4</scale>
        <Icon>
            <href>http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png</href>
        </Icon>
    </IconStyle>
	 <LabelStyle>
    <scale>0</scale>
  </LabelStyle>
</Style>
	<Style id="smalltransgray">
    <IconStyle>
		<color>461478F0</color>
		<scale>0.4</scale>
        <Icon>
            <href>http://maps.google.com/mapfiles/kml/shapes/placemark_square.png</href>
        </Icon>
    </IconStyle>
	  <LabelStyle>
    <scale>0</scale>
  </LabelStyle>
	</Style>
<Style id="smalltransblue">
    <IconStyle>
		<color>64F01414</color>
		<scale>0.4</scale>
        <Icon>
            <href>http://maps.google.com/mapfiles/kml/shapes/placemark_square.png</href>
        </Icon>
    </IconStyle>
	  <LabelStyle>
    <scale>0</scale>
  </LabelStyle>
	</Style>

';

$missCounter = 0;
$locationSample = 0;
$packetReceived = 0;
$ackReceived = 0;

while (<$fh>) {


	my @rowArray = split(/,/); 
	my $pointTime = $rowArray[4];
	my $pointLatitude = $rowArray[5];
	my $pointLongitude = $rowArray[6];
	my $pointReceived = $rowArray[7];
	my $altitude = 0;
	

	($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime($pointTime);
	$year += 1900;
	$mon++;
	
	if ($pointLatitude == 0) { 
		next; 
	} 
	
	if ($pointReceived == 1 or $pointReceived == 4) { 
		$missCounter = 0;
	} else {
		$missCounter++;
	}
	
	print "<Placemark>\n";
	print "\t<TimeStamp>\n";
	print "\t\t<when>$year-$mon-$mday" . "T" . "$hour:$min:$sec" . "Z</when>\n";
	print "\t</TimeStamp>\n";
	if ($pointReceived > 0) { 
		if ($pointReceived == 1) {
				print "\t<styleUrl>#smallgreen</styleUrl>\n";
				print "<name>Packet Received</name>\n";
				$packetReceived++;
			} else {
				print "\t<styleUrl>#smalltransblue</styleUrl>\n";
				print "<name>Acknowledgement Received</name>\n";
				$ackReceived++;
				}
		
	} else {
		if ($missCounter < 2) {
				print "\t<styleUrl>#smalltransgray</styleUrl>\n";
				print "<name>GPS Location</name>\n";
				$locationSample++;
			} else {
				print "\t<styleUrl>#smallred</styleUrl>\n";
				print "<name>Suspicious Location</name>\n";
				$locationSample++;
			}
			
	}
	print "\t<Point><coordinates>$pointLongitude,$pointLatitude,$altitude</coordinates></Point>\n";
	print "</Placemark>\n";

	#if ($pointReceived > 0) { print "SEXY: $rowArray[7] !!!!!!!!!!!!!!\n" };
}

print "</Document>\n";
print "</kml>\n";

$availReceived = ($packetReceived / $locationSample) * 100;
$transIntegrity = ($ackReceived / $packetReceived) * 100;

print "<!-- $locationSample total samples, $packetReceived packets received, $ackReceived acknowledgements. $availReceived pct availability. $transIntegrity pct of packets received response. -->";
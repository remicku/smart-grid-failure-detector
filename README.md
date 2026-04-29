# ide1-groupe-18

Situation

Comme with a startup idea of innovative service based on :
	-	IOT devices.
	-	An information system


IOT devices those devices must emit data every few minutes (or seconds). If your startups is successful there will be millions of those devices. And they will produce something like 200Go per day.
We consider that you’ve managed to make a working prototype whatever is your devices doing you.
Every data emit by the devices must contain : timestamps, device id, latitude and longitude, and any other field you find usefull

Your information system must provide :
	-	an urgent service referred after as «alert»
	-	a long term analytics service


It's up to you to report and recommend the right architecture.

your solution architecture is very likely to include :
	-	at least one distributed storage
	-	at least one distributed stream
	-	at least two stream consumer

Preliminary questions
	1.a	What technical/business constraints should the data storage component of the program architecture meet to fulfill the requirement described by the customer in paragraph «Statistics» ? 
	1.b	So what kind of component(s) (listed in the lecture) will the architecture need?
	2.a	What business constraint should the architecture meet to fulfill the requirement describe in the paragraph «Alert»? 
	2.b	Which component to choose?

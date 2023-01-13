SHELL=/bin/bash

count.png: ./count/results.json Makefile
	observable-prerender @asg017/hyperfine-benchmark-viz1 chart  \
		--redefine=width:900 \
		--width 900 \
		--redefine-file=data:json:$< \
		--redefine-file=highlight:json:<(echo '["./sqlite-xsv.sh"]') \
		'--redefine=compareTo:./sqlite-xsv.sh' \
		'--redefine=title:Count 1.1 millions rows in a 75MB CSV.' \
		'--redefine=footer:sqlite-xsv version 0.0.1, ran on a Digital Ocean Droplet 8GB RAM, 4vCPUs' \
		-o $@

aggregate.png: ./aggregate/results.json Makefile
	observable-prerender @asg017/hyperfine-benchmark-viz1 chart  \
		--redefine=width:900 \
		--width 900 \
		--redefine-file=data:json:$< \
		--redefine-file=highlight:json:<(echo '["./sqlite-xsv.sh"]') \
		'--redefine=compareTo:./sqlite-xsv.sh' \
		'--redefine=title:GROUP BY aggregation on a 226MB, 2.1 million row CSV .' \
		'--redefine=footer:sqlite-xsv version 0.0.1, ran on a Digital Ocean Droplet 8GB RAM, 4vCPUs' \
		-o $@

all: ./count.png aggregate.png

clean: 
	rm *.png

.PHONY: all clean

_data/totals.csv:
	wget -O $@ 'https://github.com/datadesk/california-coronavirus-data/raw/master/latimes-place-totals.csv'

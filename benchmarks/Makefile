SHELL=/bin/bash

count.png: ./count/results.json Makefile
	observable-prerender @asg017/hyperfine-benchmark-viz1 chart  \
		--redefine=width:900 \
		--width 900 \
		--redefine-file=data:json:$< \
		--redefine-file=highlight:json:<(echo '["./sqlite-xsv.sh"]') \
		'--redefine=compareTo:./sqlite-xsv.sh' \
		'--redefine=title:Count 1.1 millions rows in a 75MB CSV.' \
		'--redefine=footer:sqlite-xsv version 0.0.1, ran on a 2019 Macbook, i5 processing and 8GB RAM' \
		-o $@

all: ./count.png 

clean: 
	rm *.png

.PHONY: all clean

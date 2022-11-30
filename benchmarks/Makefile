SHELL=/bin/bash

count.png: ./count/results.json Makefile
	observable-prerender @asg017/hyperfine-benchmark-viz1 chart  \
		--redefine=width:800 \
		--width 825 \
		--redefine-file=data:json:$< \
		--redefine-file=highlight:json:<(echo '["./xsv.sh"]') \
		'--redefine=compareTo:./xsv.sh' \
		'--redefine=title:TODO' \
		'--redefine=footer:TODO' \
		-o $@

all: ./count.png 

clean: 
	rm *.png

.PHONY: all clean
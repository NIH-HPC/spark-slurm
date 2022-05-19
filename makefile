.ONESHELL:


deploy: spark pyspark-jupyter
ifdef ver
	$(eval prefix := /usr/local/Spark/$(ver))
else
	$(info Usage:  make deploy ver=X.X.X)
	$(error    ver is required)
endif
	sed -e 's:^soc_sparkdir=.*$$:soc_sparkdir=/data/$$USER/.spark-on-biowulf:' \
		-e 's:^soc_headnode=.*$$:soc_headnode=biowulf.nih.gov:' \
		-e 's:^soc_sparkver=.*$$:soc_sparkver=$(ver):' $< > tmp_$<
	install -m 755 --backup tmp_$< $(prefix)/bin/$<
	install -m 755 --backup $(word 2,$^) $(prefix)/bin/$(word 2,$^)
	rm tmp_$<

